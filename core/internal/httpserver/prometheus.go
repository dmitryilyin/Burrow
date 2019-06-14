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
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/linkedin/Burrow/core/protocol"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"net/http"
	"os"
	"strconv"
)

func (hc *Coordinator) handlePrometheusMetrics(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {

	// Metric Family declarations
	
	// Consumer Group
	
	consumerGroupTotalLagMetricFamilyName := "burrow_consumer_group_total_lag"
	consumerGroupTotalLagMetricFamilyHelp := "The sum of all consumed partition current lag values for the group."
	consumerGroupTotalLagMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupTotalLagMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupTotalLagMetricFamilyName,
		Help: &consumerGroupTotalLagMetricFamilyHelp,
		Type: &consumerGroupTotalLagMetricFamilyType,
	}

	consumerGroupTotalPartitionsMetricFamilyName := "burrow_consumer_group_total_partitions"
	consumerGroupTotalPartitionsMetricFamilyHelp := "The total count of all partitions that this groups has committed offsets for across all consumed topics. It may not be the same as the total number of partitions consumed by the group, if Burrow has not seen commits for all partitions yet."
	consumerGroupTotalPartitionsMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupTotalPartitionsMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupTotalPartitionsMetricFamilyName,
		Help: &consumerGroupTotalPartitionsMetricFamilyHelp,
		Type: &consumerGroupTotalPartitionsMetricFamilyType,
	}

	consumerGroupStatusCodeMetricFamilyName := "burrow_consumer_group_status_code"
	consumerGroupStatusCodeMetricFamilyHelp := "The status code of the consumer group. It is calculated from the highest status for the individual partitions. (Codes are index of this list starting from zero: NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND)."
	consumerGroupStatusCodeMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupStatusCodeMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupStatusCodeMetricFamilyName,
		Help: &consumerGroupStatusCodeMetricFamilyHelp,
		Type: &consumerGroupStatusCodeMetricFamilyType,
	}

	consumerGroupMaxLagMetricFamilyName := "burrow_consumer_group_max_lag"
	consumerGroupMaxLagMetricFamilyHelp := "The current lag value of a partition consumed by this group which has the highest lag."
	consumerGroupMaxLagMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupMaxLagMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupMaxLagMetricFamilyName,
		Help: &consumerGroupMaxLagMetricFamilyHelp,
		Type: &consumerGroupMaxLagMetricFamilyType,
	}

	// Consumer Group Partition

	consumerGroupPartitionCurrentLagMetricFamilyName := "burrow_consumer_group_partition_current_lag"
	consumerGroupPartitionCurrentLagMetricFamilyHelp := "The current number of messages that the consumer is behind for this partition. This is calculated using the last committed offset and the current broker end offset."
	consumerGroupPartitionCurrentLagMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupPartitionCurrentLagMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupPartitionCurrentLagMetricFamilyName,
		Help: &consumerGroupPartitionCurrentLagMetricFamilyHelp,
		Type: &consumerGroupPartitionCurrentLagMetricFamilyType,
	}

	consumerGroupPartitionStatusCodeMetricFamilyName := "burrow_consumer_group_partition_status_code"
	consumerGroupPartitionStatusCodeMetricFamilyHelp := "The status code of the consumer group partition. (Codes are index of this list starting from zero: NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND)."
	consumerGroupPartitionStatusCodeMetricFamilyType := dto.MetricType_GAUGE
	consumerGroupPartitionStatusCodeMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupPartitionStatusCodeMetricFamilyName,
		Help: &consumerGroupPartitionStatusCodeMetricFamilyHelp,
		Type: &consumerGroupPartitionStatusCodeMetricFamilyType,
	}

	consumerGroupPartitionLatestOffsetMetricFamilyName := "burrow_consumer_group_partition_latest_offset"
	consumerGroupPartitionLatestOffsetMetricFamilyHelp := "The latest offset value that Burrow is storing for this partition."
	consumerGroupPartitionLatestOffsetMetricFamilyType := dto.MetricType_COUNTER
	consumerGroupPartitionLatestOffsetMetricFamily := &dto.MetricFamily{
		Name: &consumerGroupPartitionLatestOffsetMetricFamilyName,
		Help: &consumerGroupPartitionLatestOffsetMetricFamilyHelp,
		Type: &consumerGroupPartitionLatestOffsetMetricFamilyType,
	}
	
	// Topic Partition

	topicPartitionOffsetMetricFamilyName := "burrow_topic_partition_offset"
	topicPartitionOffsetMetricFamilyHelp := "The offset of the partition in this topic."
	topicPartitionOffsetMetricFamilyType := dto.MetricType_COUNTER
	topicPartitionOffsetMetricFamily := &dto.MetricFamily{
		Name: &topicPartitionOffsetMetricFamilyName,
		Help: &topicPartitionOffsetMetricFamilyHelp,
		Type: &topicPartitionOffsetMetricFamilyType,
	}

	metricFamilies := []*dto.MetricFamily{
		consumerGroupTotalLagMetricFamily,
		consumerGroupTotalPartitionsMetricFamily,
		consumerGroupStatusCodeMetricFamily,
		consumerGroupMaxLagMetricFamily,
		consumerGroupPartitionCurrentLagMetricFamily,
		consumerGroupPartitionStatusCodeMetricFamily,
		consumerGroupPartitionLatestOffsetMetricFamily,
		topicPartitionOffsetMetricFamily,
	}

	// Label names
	hostLabelName := "host"
	clusterLabelName := "cluster"
	consumerGroupLabelName := "consumer_group"
	consumerGroupStatusLabelName := "consumer_group_status"

	topicLabelName := "topic"
	partitionLabelName := "partition"
	partitionStatusLabelName := "partition_status"
	ownerLabelName := "owner"
	clientIdLabelName := "client_id"

	// Fetch cluster list from the storage module
	requestFetchClusters := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- requestFetchClusters
	responseFetchClusters := <-requestFetchClusters.Reply

	host, _ := os.Hostname()

	for _, cluster := range responseFetchClusters.([]string) {
		clusterLabelValue := cluster

		// Consumer Group List

		requestFetchConsumers := &protocol.StorageRequest{
			RequestType: protocol.StorageFetchConsumers,
			Cluster:     cluster,
			Reply:       make(chan interface{}),
		}
		hc.App.StorageChannel <- requestFetchConsumers
		responseFetchConsumers := <-requestFetchConsumers.Reply

		// Consumer Group Status

		for _, consumerGroup := range responseFetchConsumers.([]string) {
			consumerGroupLabelValue := consumerGroup

			// request the consumer group status structure
			requestConsumerGroupStatus := &protocol.EvaluatorRequest{
				Cluster: cluster,
				Group:   consumerGroup,
				ShowAll: true,
				Reply:   make(chan *protocol.ConsumerGroupStatus),
			}
			hc.App.EvaluatorChannel <- requestConsumerGroupStatus
			responseConsumerGroupStatus := <-requestConsumerGroupStatus.Reply

			// skip the consumer group if the data have not been fully gathered
			if responseConsumerGroupStatus.Complete < 1.0 {
				continue
			}

			consumerGroupStatusLabelValue := responseConsumerGroupStatus.Status.String()

			consumerGroupLabels := []*dto.LabelPair{
				{ Name: &hostLabelName, Value: &host },
				{ Name: &clusterLabelName, Value: &clusterLabelValue },
				{ Name: &consumerGroupLabelName, Value: &consumerGroupLabelValue },
				{ Name: &consumerGroupStatusLabelName, Value: &consumerGroupStatusLabelValue },
			}

			consumerGroupTotalLagMetricValue := float64(responseConsumerGroupStatus.TotalLag)
			consumerGroupTotalLagMetric := dto.Metric{
				Gauge: &dto.Gauge{ Value: &consumerGroupTotalLagMetricValue },
				Label: consumerGroupLabels,
			}
			consumerGroupTotalLagMetricFamily.Metric = append(consumerGroupTotalLagMetricFamily.Metric, &consumerGroupTotalLagMetric)

			consumerGroupTotalPartitionsMetricValue := float64(responseConsumerGroupStatus.TotalPartitions)
			consumerGroupTotalPartitionsMetric := dto.Metric{
				Gauge: &dto.Gauge{ Value: &consumerGroupTotalPartitionsMetricValue },
				Label: consumerGroupLabels,				
			}
			consumerGroupTotalPartitionsMetricFamily.Metric = append(consumerGroupTotalPartitionsMetricFamily.Metric, &consumerGroupTotalPartitionsMetric)

			consumerGroupStatusCodeMetricValue :=  float64(responseConsumerGroupStatus.Status)
			consumerGroupStatusCodeMetric := dto.Metric{
				Gauge: &dto.Gauge{ Value: &consumerGroupStatusCodeMetricValue },
				Label: consumerGroupLabels,
			}
			consumerGroupStatusCodeMetricFamily.Metric = append(consumerGroupStatusCodeMetricFamily.Metric, &consumerGroupStatusCodeMetric)

			if responseConsumerGroupStatus.Maxlag != nil {
				consumerGroupMaxLagMetricValue := float64(responseConsumerGroupStatus.Maxlag.CurrentLag)
				consumerGroupMaxLagMetric := dto.Metric{
					Gauge: &dto.Gauge{Value: &consumerGroupMaxLagMetricValue},
					Label: consumerGroupLabels,
				}
				consumerGroupMaxLagMetricFamily.Metric = append(consumerGroupMaxLagMetricFamily.Metric, &consumerGroupMaxLagMetric)
			}

			// Consumer Group Partition Status

			for _, partitionStatus := range responseConsumerGroupStatus.Partitions {

				// skip the consumer group partition if the data have not been fully gathered
				if partitionStatus.Complete < 1.0 {
					continue
				}
				
				topicLabelValue := partitionStatus.Topic
				partitionLabelValue := strconv.Itoa(int(partitionStatus.Partition))
				partitionStatusLabelValue := partitionStatus.Status.String()
				ownerLabelValue := partitionStatus.Owner
				clientIdLabelValue := partitionStatus.ClientID
				
				consumerGroupPartitionLabels := []*dto.LabelPair{
					{ Name: &hostLabelName, Value: &host },
					{ Name: &clusterLabelName, Value: &clusterLabelValue },
					{ Name: &consumerGroupLabelName, Value: &consumerGroupLabelValue },
					{ Name: &consumerGroupStatusLabelName, Value: &consumerGroupStatusLabelValue },
					{ Name: &topicLabelName, Value: &topicLabelValue },
					{ Name: &partitionLabelName, Value: &partitionLabelValue },
					{ Name: &partitionStatusLabelName, Value: &partitionStatusLabelValue },
					{ Name: &ownerLabelName, Value: &ownerLabelValue},
					{ Name: &clientIdLabelName, Value: &clientIdLabelValue },
				}

				consumerGroupPartitionCurrentLagMetricValue := float64(partitionStatus.CurrentLag)
				consumerGroupPartitionCurrentLagMetric := dto.Metric{
					Gauge: &dto.Gauge{ Value: &consumerGroupPartitionCurrentLagMetricValue },
					Label: consumerGroupPartitionLabels,
				}
				consumerGroupPartitionCurrentLagMetricFamily.Metric = append(consumerGroupPartitionCurrentLagMetricFamily.Metric, &consumerGroupPartitionCurrentLagMetric)

				consumerGroupPartitionStatusCodeMetricValue := float64(partitionStatus.Status)
				consumerGroupPartitionStatusCodeMetric := dto.Metric{
					Gauge: &dto.Gauge{ Value: &consumerGroupPartitionStatusCodeMetricValue },
					Label: consumerGroupPartitionLabels,
				}
				consumerGroupPartitionStatusCodeMetricFamily.Metric = append(consumerGroupPartitionStatusCodeMetricFamily.Metric, &consumerGroupPartitionStatusCodeMetric)

				consumerGroupPartitionLatestOffsetMetricValue := float64(partitionStatus.End.Offset)
				consumerGroupPartitionLatestOffsetMetric := dto.Metric{
					Counter: &dto.Counter{ Value: &consumerGroupPartitionLatestOffsetMetricValue },
					Label: consumerGroupPartitionLabels,
				}
				consumerGroupPartitionLatestOffsetMetricFamily.Metric = append(consumerGroupPartitionLatestOffsetMetricFamily.Metric, &consumerGroupPartitionLatestOffsetMetric)

			}
		}

		// Topic List

		requestFetchTopics := &protocol.StorageRequest{
			RequestType: protocol.StorageFetchTopics,
			Cluster:     cluster,
			Reply:       make(chan interface{}),
		}
		hc.App.StorageChannel <- requestFetchTopics
		responseFetchTopics := <-requestFetchTopics.Reply

		// Topic Status

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
					{ Name: &hostLabelName, Value: &host },
					{ Name: &clusterLabelName, Value: &clusterLabelValue },
					{ Name: &topicLabelName, Value: &topicLabelValue },
					{ Name: &partitionLabelName, Value: &partitionLabelValue },
				}

				topicPartitionOffsetMetricValue := float64(partitionOffset)
				topicPartitionOffsetMetric := dto.Metric{
					Counter: &dto.Counter{ Value: &topicPartitionOffsetMetricValue },
					Label: topicPartitionLabels,
				}
				topicPartitionOffsetMetricFamily.Metric = append(topicPartitionOffsetMetricFamily.Metric, &topicPartitionOffsetMetric)

			}

		}

	}

	// Output

	contentType := expfmt.Negotiate(request.Header)

	header := writer.Header()
	header.Set("Content-Type", string(contentType))
	header.Set("Access-Control-Allow-Origin", "*")

	encoder := expfmt.NewEncoder(writer, contentType)

	for _, metricFamily := range metricFamilies {
		err := encoder.Encode(metricFamily)
		if err != nil {
			http.Error(writer, fmt.Sprintf("Error encoding metrics: %v", err), http.StatusInternalServerError)
		}
	}

	writer.WriteHeader(http.StatusOK)

}
