using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaPipeline.Core.Messaging;

public static class KafkaLoggingExtensions
{
    public static void LogKafkaError(this ILogger logger, Error error)
    {
        if (error.IsFatal)
        {
            logger.LogError("Kafka fatal error: {Reason} ({Code})", error.Reason, error.Code);
        }
        else if (error.IsBrokerError)
        {
            logger.LogWarning("Kafka broker error: {Reason} ({Code})", error.Reason, error.Code);
        }
        else
        {
            logger.LogWarning("Kafka client notice: {Reason} ({Code})", error.Reason, error.Code);
        }
    }

    /// <summary>
    /// Handles librdkafka statistics callbacks. Summarizes consumer lag and
    /// broker connectivity at Debug level. Never throws: malformed payloads
    /// are logged at Debug and ignored.
    /// </summary>
    public static void LogKafkaStatistics(this ILogger logger, string statisticsJson)
    {
        if (string.IsNullOrWhiteSpace(statisticsJson))
        {
            return;
        }

        try
        {
            using var document = JsonDocument.Parse(statisticsJson);
            var root = document.RootElement;

            var clientId = root.TryGetProperty("client_id", out var clientIdElement)
                && clientIdElement.ValueKind == JsonValueKind.String
                    ? clientIdElement.GetString()
                    : null;
            if (string.IsNullOrEmpty(clientId) && root.TryGetProperty("name", out var nameElement)
                && nameElement.ValueKind == JsonValueKind.String)
            {
                clientId = nameElement.GetString();
            }

            var totalLag = SumConsumerLag(root);

            var brokerCount = 0;
            var brokersDown = 0;
            if (root.TryGetProperty("brokers", out var brokers)
                && brokers.ValueKind == JsonValueKind.Object)
            {
                foreach (var broker in brokers.EnumerateObject())
                {
                    brokerCount++;
                    if (broker.Value.ValueKind == JsonValueKind.Object
                        && broker.Value.TryGetProperty("state", out var state)
                        && state.ValueKind == JsonValueKind.String
                        && !string.Equals(state.GetString(), "UP", StringComparison.OrdinalIgnoreCase))
                    {
                        brokersDown++;
                    }
                }
            }

            logger.LogDebug(
                "Kafka statistics for {ClientId}: consumer lag {ConsumerLag} across reported partitions, brokers down {BrokersDown}/{BrokerCount}",
                clientId ?? "unknown",
                totalLag,
                brokersDown,
                brokerCount);
        }
        catch (JsonException exception)
        {
            logger.LogDebug(exception, "Failed to parse Kafka statistics payload ({Length} chars)", statisticsJson.Length);
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "Failed to process Kafka statistics payload");
        }
    }

    private static long SumConsumerLag(JsonElement root)
    {
        var totalLag = 0L;

        if (!root.TryGetProperty("topics", out var topics)
            || topics.ValueKind != JsonValueKind.Object)
        {
            return totalLag;
        }

        foreach (var topic in topics.EnumerateObject())
        {
            if (topic.Value.ValueKind != JsonValueKind.Object
                || !topic.Value.TryGetProperty("partitions", out var partitions)
                || partitions.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            foreach (var partition in partitions.EnumerateObject())
            {
                // consumer_lag is -1 when unknown; only non-negative values are meaningful.
                if (partition.Value.ValueKind == JsonValueKind.Object
                    && partition.Value.TryGetProperty("consumer_lag", out var lag)
                    && lag.ValueKind == JsonValueKind.Number
                    && lag.TryGetInt64(out var lagValue)
                    && lagValue > 0)
                {
                    totalLag += lagValue;
                }
            }
        }

        return totalLag;
    }
}
