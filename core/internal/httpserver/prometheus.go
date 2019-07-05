/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package httpserver

import (
	"compress/gzip"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/linkedin/Burrow/core/protocol"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (hc *Coordinator) prometheusMetricsDefaults() {
	viper.SetDefault("metrics.prometheus.metric-consumer-enabled", true)
	viper.SetDefault("metrics.prometheus.metric-consumer-total-lag", "burrow_consumer_group_total_lag")
	viper.SetDefault("metrics.prometheus.metric-consumer-partitions", "burrow_consumer_group_total_partitions")
	viper.SetDefault("metrics.prometheus.metric-consumer-status", "burrow_consumer_group_status_code")
	viper.SetDefault("metrics.prometheus.metric-consumer-max-lag", "burrow_consumer_group_max_lag")

	viper.SetDefault("metrics.prometheus.metric-partition-enabled", true)
	viper.SetDefault("metrics.prometheus.metric-partition-current-lag", "burrow_consumer_group_partition_current_lag")
	viper.SetDefault("metrics.prometheus.metric-partition-status", "burrow_consumer_group_partition_status_code")
	viper.SetDefault("metrics.prometheus.metric-partition-offset", "burrow_consumer_group_partition_latest_offset")

	viper.SetDefault("metrics.prometheus.metric-topic-enabled", true)
	viper.SetDefault("metrics.prometheus.metric-topic-offset", "burrow_topic_partition_offset")

	viper.SetDefault("metrics.prometheus.only-complete-consumers", false)
	viper.SetDefault("metrics.prometheus.only-complete-partitions", false)

	viper.SetDefault("metrics.prometheus.include-topics", "")
	viper.SetDefault("metrics.prometheus.exclude-topics", "")
	viper.SetDefault("metrics.prometheus.include-consumers", "")
	viper.SetDefault("metrics.prometheus.exclude-consumers", "")
}

func (hc *Coordinator) prometheusHandleMetrics(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	metricFamilies := make([]*dto.MetricFamily, 8)

	metricConsumerEnabled := viper.GetBool("metrics.prometheus.metric-consumer-enabled")
	metricPartitionEnabled := viper.GetBool("metrics.prometheus.metric-partition-enabled")
	metricTopicEnabled := viper.GetBool("metrics.prometheus.metric-topic-enabled")

	requestFetchClusters := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- requestFetchClusters
	responseFetchClusters := <-requestFetchClusters.Reply

	host, _ := os.Hostname()

	for _, cluster := range responseFetchClusters.([]string) {

		if metricConsumerEnabled || metricPartitionEnabled {
			hc.prometheusCollectConsumerMetrics(metricFamilies, metricConsumerEnabled, metricPartitionEnabled, host, cluster)
		}

		if metricTopicEnabled {
			hc.prometheusCollectTopicMetrics(metricFamilies, host, cluster)
		}

	}

	hc.prometheusMetricsOutput(writer, request, metricFamilies)

}

func (hc *Coordinator) prometheusCollectConsumerMetrics(metricFamilies []*dto.MetricFamily, metricConsumerEnabled bool, metricPartitionEnabled bool, host string, cluster string) {

	hostLabelName := "host"
	clusterLabelName := "cluster"
	consumerGroupLabelName := "consumer_group"
	consumerGroupStatusLabelName := "consumer_group_status"

	consumerGroupTotalLagMetricFamilyName := viper.GetString("metrics.prometheus.metric-consumer-total-lag")
	consumerGroupTotalLagMetricFamilyHelp := "The sum of all consumed partition current lag values for the group."
	consumerGroupTotalLagMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupTotalLagMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupTotalLagMetricFamilyName,
		Help: &consumerGroupTotalLagMetricFamilyHelp,
		Type: &consumerGroupTotalLagMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupTotalLagMetricFamily)

	consumerGroupTotalPartitionsMetricFamilyName := viper.GetString("metrics.prometheus.metric-consumer-partitions")
	consumerGroupTotalPartitionsMetricFamilyHelp := "The total count of all partitions that this groups has committed offsets for across all consumed topics. It may not be the same as the total number of partitions consumed by the group, if Burrow has not seen commits for all partitions yet."
	consumerGroupTotalPartitionsMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupTotalPartitionsMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupTotalPartitionsMetricFamilyName,
		Help: &consumerGroupTotalPartitionsMetricFamilyHelp,
		Type: &consumerGroupTotalPartitionsMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupTotalPartitionsMetricFamily)

	consumerGroupStatusCodeMetricFamilyName := viper.GetString("metrics.prometheus.metric-consumer-status")
	consumerGroupStatusCodeMetricFamilyHelp := "The status code of the consumer group. It is calculated from the highest status for the individual partitions. (Codes are index of this list starting from zero: NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND)."
	consumerGroupStatusCodeMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupStatusCodeMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupStatusCodeMetricFamilyName,
		Help: &consumerGroupStatusCodeMetricFamilyHelp,
		Type: &consumerGroupStatusCodeMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupStatusCodeMetricFamily)

	consumerGroupMaxLagMetricFamilyName := viper.GetString("metrics.prometheus.metric-consumer-max-lag")
	consumerGroupMaxLagMetricFamilyHelp := "The current lag value of a partition consumed by this group which has the highest lag."
	consumerGroupMaxLagMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupMaxLagMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupMaxLagMetricFamilyName,
		Help: &consumerGroupMaxLagMetricFamilyHelp,
		Type: &consumerGroupMaxLagMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupMaxLagMetricFamily)

	requestFetchConsumers := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumers,
		Cluster:     cluster,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- requestFetchConsumers
	responseFetchConsumers := <-requestFetchConsumers.Reply

	for _, consumerGroup := range responseFetchConsumers.([]string) {
		consumerGroupLabelValue := consumerGroup

		requestConsumerGroupStatus := &protocol.EvaluatorRequest{
			Cluster: cluster,
			Group:   consumerGroup,
			ShowAll: true,
			Reply:   make(chan *protocol.ConsumerGroupStatus),
		}
		hc.App.EvaluatorChannel <- requestConsumerGroupStatus
		responseConsumerGroupStatus := <-requestConsumerGroupStatus.Reply

		if viper.GetBool("metrics.prometheus.only-complete-consumers") {
			if responseConsumerGroupStatus.Complete < 1.0 {
				continue
			}
		}

		if metricConsumerEnabled {
			consumerGroupStatusLabelValue := responseConsumerGroupStatus.Status.String()

			consumerGroupLabels := []*dto.LabelPair{
				{Name: &hostLabelName, Value: &host},
				{Name: &clusterLabelName, Value: &cluster},
				{Name: &consumerGroupLabelName, Value: &consumerGroupLabelValue},
				{Name: &consumerGroupStatusLabelName, Value: &consumerGroupStatusLabelValue},
			}

			if consumerGroupTotalLagMetricFamilyName != "" {
				consumerGroupTotalLagMetricValue := float64(responseConsumerGroupStatus.TotalLag)
				consumerGroupTotalLagMetric := dto.Metric{
					Gauge: &dto.Gauge{Value: &consumerGroupTotalLagMetricValue},
					Label: consumerGroupLabels,
				}
				consumerGroupTotalLagMetricFamily.Metric = append(consumerGroupTotalLagMetricFamily.Metric, &consumerGroupTotalLagMetric)
			}

			if consumerGroupTotalPartitionsMetricFamilyName != "" {
				consumerGroupTotalPartitionsMetricValue := float64(responseConsumerGroupStatus.TotalPartitions)
				consumerGroupTotalPartitionsMetric := dto.Metric{
					Gauge: &dto.Gauge{Value: &consumerGroupTotalPartitionsMetricValue},
					Label: consumerGroupLabels,
				}
				consumerGroupTotalPartitionsMetricFamily.Metric = append(consumerGroupTotalPartitionsMetricFamily.Metric, &consumerGroupTotalPartitionsMetric)
			}

			if consumerGroupStatusCodeMetricFamilyName != "" {
				consumerGroupStatusCodeMetricValue := float64(responseConsumerGroupStatus.Status)
				consumerGroupStatusCodeMetric := dto.Metric{
					Gauge: &dto.Gauge{Value: &consumerGroupStatusCodeMetricValue},
					Label: consumerGroupLabels,
				}
				consumerGroupStatusCodeMetricFamily.Metric = append(consumerGroupStatusCodeMetricFamily.Metric, &consumerGroupStatusCodeMetric)
			}

			if consumerGroupMaxLagMetricFamilyName != "" && responseConsumerGroupStatus.Maxlag != nil {
				consumerGroupMaxLagMetricValue := float64(responseConsumerGroupStatus.Maxlag.CurrentLag)
				consumerGroupMaxLagMetric := dto.Metric{
					Gauge: &dto.Gauge{Value: &consumerGroupMaxLagMetricValue},
					Label: consumerGroupLabels,
				}
				consumerGroupMaxLagMetricFamily.Metric = append(consumerGroupMaxLagMetricFamily.Metric, &consumerGroupMaxLagMetric)
			}
		}

		if metricPartitionEnabled {
			hc.prometheusCollectPartitionMetrics(
				metricFamilies,
				consumerGroup,
				responseConsumerGroupStatus,
				host,
				cluster,
			)
		}

	}

}

func (hc *Coordinator) prometheusCollectPartitionMetrics(metricFamilies []*dto.MetricFamily, consumerGroup string, responseConsumerGroupStatus *protocol.ConsumerGroupStatus, host string, cluster string) {

	hostLabelName := "host"
	clusterLabelName := "cluster"
	consumerGroupLabelName := "consumer_group"
	consumerGroupStatusLabelName := "consumer_group_status"

	topicLabelName := "topic"
	partitionLabelName := "partition"
	partitionStatusLabelName := "partition_status"
	ownerLabelName := "owner"
	clientIdLabelName := "client_id"

	consumerGroupPartitionCurrentLagMetricFamilyName := viper.GetString("metrics.prometheus.metric-partition-current-lag")
	consumerGroupPartitionCurrentLagMetricFamilyHelp := "The current number of messages that the consumer is behind for this partition. This is calculated using the last committed offset and the current broker end offset."
	consumerGroupPartitionCurrentLagMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupPartitionCurrentLagMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupPartitionCurrentLagMetricFamilyName,
		Help: &consumerGroupPartitionCurrentLagMetricFamilyHelp,
		Type: &consumerGroupPartitionCurrentLagMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupPartitionCurrentLagMetricFamily)

	consumerGroupPartitionStatusCodeMetricFamilyName := viper.GetString("metrics.prometheus.metric-partition-status")
	consumerGroupPartitionStatusCodeMetricFamilyHelp := "The status code of the consumer group partition. (Codes are index of this list starting from zero: NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND)."
	consumerGroupPartitionStatusCodeMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupPartitionStatusCodeMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupPartitionStatusCodeMetricFamilyName,
		Help: &consumerGroupPartitionStatusCodeMetricFamilyHelp,
		Type: &consumerGroupPartitionStatusCodeMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupPartitionStatusCodeMetricFamily)

	consumerGroupPartitionLatestOffsetMetricFamilyName := viper.GetString("metrics.prometheus.metric-partition-offset")
	consumerGroupPartitionLatestOffsetMetricFamilyHelp := "The latest offset value that Burrow is storing for this partition."
	consumerGroupPartitionLatestOffsetMetricFamilyType := dto.MetricType_COUNTER
	consumerGroupPartitionLatestOffsetMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupPartitionLatestOffsetMetricFamilyName,
		Help: &consumerGroupPartitionLatestOffsetMetricFamilyHelp,
		Type: &consumerGroupPartitionLatestOffsetMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, consumerGroupPartitionLatestOffsetMetricFamily)

	for _, partitionStatus := range responseConsumerGroupStatus.Partitions {

		if viper.GetBool("metrics.prometheus.only-complete-partitions") {
			if partitionStatus.Complete < 1.0 {
				continue
			}
		}

		topicLabelValue := partitionStatus.Topic
		partitionLabelValue := strconv.Itoa(int(partitionStatus.Partition))
		partitionStatusLabelValue := partitionStatus.Status.String()
		ownerLabelValue := partitionStatus.Owner
		clientIdLabelValue := partitionStatus.ClientID

		consumerGroupStatusLabelValue := responseConsumerGroupStatus.Status.String()

		consumerGroupPartitionLabels := []*dto.LabelPair{
			{Name: &hostLabelName, Value: &host},
			{Name: &clusterLabelName, Value: &cluster},
			{Name: &consumerGroupLabelName, Value: &consumerGroup},
			{Name: &consumerGroupStatusLabelName, Value: &consumerGroupStatusLabelValue},
			{Name: &topicLabelName, Value: &topicLabelValue},
			{Name: &partitionLabelName, Value: &partitionLabelValue},
			{Name: &partitionStatusLabelName, Value: &partitionStatusLabelValue},
			{Name: &ownerLabelName, Value: &ownerLabelValue},
			{Name: &clientIdLabelName, Value: &clientIdLabelValue},
		}

		if consumerGroupPartitionCurrentLagMetricFamilyName != "" {
			consumerGroupPartitionCurrentLagMetricValue := float64(partitionStatus.CurrentLag)
			consumerGroupPartitionCurrentLagMetric := dto.Metric{
				Gauge: &dto.Gauge{Value: &consumerGroupPartitionCurrentLagMetricValue},
				Label: consumerGroupPartitionLabels,
			}
			consumerGroupPartitionCurrentLagMetricFamily.Metric = append(consumerGroupPartitionCurrentLagMetricFamily.Metric, &consumerGroupPartitionCurrentLagMetric)
		}

		if consumerGroupPartitionStatusCodeMetricFamilyName != "" {
			consumerGroupPartitionStatusCodeMetricValue := float64(partitionStatus.Status)
			consumerGroupPartitionStatusCodeMetric := dto.Metric{
				Gauge: &dto.Gauge{Value: &consumerGroupPartitionStatusCodeMetricValue},
				Label: consumerGroupPartitionLabels,
			}
			consumerGroupPartitionStatusCodeMetricFamily.Metric = append(consumerGroupPartitionStatusCodeMetricFamily.Metric, &consumerGroupPartitionStatusCodeMetric)
		}

		if consumerGroupPartitionLatestOffsetMetricFamilyName != "" && partitionStatus.End != nil {
			consumerGroupPartitionLatestOffsetMetricValue := float64(partitionStatus.End.Offset)
			consumerGroupPartitionLatestOffsetMetric := dto.Metric{
				Counter: &dto.Counter{Value: &consumerGroupPartitionLatestOffsetMetricValue},
				Label:   consumerGroupPartitionLabels,
			}
			consumerGroupPartitionLatestOffsetMetricFamily.Metric = append(consumerGroupPartitionLatestOffsetMetricFamily.Metric, &consumerGroupPartitionLatestOffsetMetric)
		}

	}
}

func (hc *Coordinator) prometheusCollectTopicMetrics(metricFamilies []*dto.MetricFamily, host string, cluster string) {

	hostLabelName := "host"
	clusterLabelName := "cluster"
	topicLabelName := "topic"
	partitionLabelName := "partition"

	topicPartitionOffsetMetricFamilyName := viper.GetString("metrics.prometheus.metric-topic-offset")
	topicPartitionOffsetMetricFamilyHelp := "The offset of the partition in this topic."
	topicPartitionOffsetMetricFamilyType := dto.MetricType_COUNTER
	topicPartitionOffsetMetricFamily := &dto.MetricFamily{
		Name: &topicPartitionOffsetMetricFamilyName,
		Help: &topicPartitionOffsetMetricFamilyHelp,
		Type: &topicPartitionOffsetMetricFamilyType,
	}
	metricFamilies = append(metricFamilies, topicPartitionOffsetMetricFamily)

	requestFetchTopics := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopics,
		Cluster:     cluster,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- requestFetchTopics
	responseFetchTopics := <-requestFetchTopics.Reply

	for _, topic := range responseFetchTopics.([]string) {

		requestTopicDetail := &protocol.StorageRequest{
			RequestType: protocol.StorageFetchTopic,
			Cluster:     cluster,
			Topic:       topic,
			Reply:       make(chan interface{}),
		}
		hc.App.StorageChannel <- requestTopicDetail
		responseTopicDetail := <-requestTopicDetail.Reply

		for partitionNumber, partitionOffset := range responseTopicDetail.([]int64) {

			topicLabelValue := topic
			partitionLabelValue := strconv.Itoa(partitionNumber)

			topicPartitionLabels := []*dto.LabelPair{
				{Name: &hostLabelName, Value: &host},
				{Name: &clusterLabelName, Value: &cluster},
				{Name: &topicLabelName, Value: &topicLabelValue},
				{Name: &partitionLabelName, Value: &partitionLabelValue},
			}

			if topicPartitionOffsetMetricFamilyName != "" {
				topicPartitionOffsetMetricValue := float64(partitionOffset)
				topicPartitionOffsetMetric := dto.Metric{
					Counter: &dto.Counter{Value: &topicPartitionOffsetMetricValue},
					Label:   topicPartitionLabels,
				}
				topicPartitionOffsetMetricFamily.Metric = append(topicPartitionOffsetMetricFamily.Metric, &topicPartitionOffsetMetric)
			}

		}

	}

}

func (hc *Coordinator) prometheusMetricsOutput(writer http.ResponseWriter, request *http.Request, metricFamilies []*dto.MetricFamily) {
	var encoder expfmt.Encoder

	contentType := expfmt.Negotiate(request.Header)
	writer.Header().Set("Content-Type", string(contentType))

	acceptEncoding := request.Header.Get("Accept-Encoding")

	if strings.Contains(acceptEncoding, "gzip") {
		writer.Header().Set("Content-Encoding", "gzip")
		gzWriter := gzip.NewWriter(writer)
		defer gzWriter.Close()
		encoder = expfmt.NewEncoder(gzipResponseWriter{Writer: gzWriter, ResponseWriter: writer}, contentType)
	} else {
		encoder = expfmt.NewEncoder(writer, contentType)
	}

	for _, metricFamily := range metricFamilies {
		err := encoder.Encode(metricFamily)
		if err != nil {
			http.Error(writer, fmt.Sprintf("Error encoding metrics: %v", err), http.StatusInternalServerError)
		}
	}

	writer.WriteHeader(http.StatusOK)
}
