using CryptoExchange.Net;
using CryptoExchange.Net.Clients;
using CryptoExchange.Net.Interfaces;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.Sockets;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;

namespace Bybit.Net.Objects.Sockets
{
    public class SocketConnection : CryptoExchange.Net.Sockets.SocketConnection
    {
        private readonly bool _unsafeSub = false;
        private int _snapshotMessageBegin = -1;
        private int _messageTypeBegin = -1;
        private int _deltaMessageBegin = -1;

        private ILogger _logger;

        private readonly IMessageSerializer _serializer;
        private readonly IByteMessageAccessor _accessor;
        public SocketConnection(ILogger logger, SocketApiClient apiClient, IWebsocket socket, string tag, bool unsafeSubscribe = false) : base(logger, apiClient, socket, tag)
        {
            _unsafeSub = unsafeSubscribe;
            _logger = logger;
        }
        protected override async Task HandleStreamMessage(WebSocketMessageType type, ReadOnlyMemory<byte> data)
        {
            // If we choose to do it in the current format, use the base class Handling
            if (!_unsafeSub)
            {
                await base.HandleStreamMessage(type, data).ConfigureAwait(false);
                return;
            }
            var sw = Stopwatch.StartNew();
            var receiveTime = DateTime.UtcNow;
            string? originalData = null;

            // 1. Decrypt/Preprocess if necessary
            data = ApiClient.PreprocessStreamMessage(this, type, data);

            
            byte colonByte = 0x3A;
            byte stringByte = 0x22;

            if(_messageTypeBegin == -1)
            {
                int quotations = 0;
                for(int i = 0; i < data.Length; i++)
                {
                    // Message type can be found after 2nd colon for orderbook messages
                    // We look for the colon
                    if (data.Span[i] == stringByte)
                    {
                        quotations++;
                        // Start of message string
                        if(quotations == 7)
                        {
                            _messageTypeBegin = i + 1;
                            break;
                        }
                    }
                }
            }
            string messageType = string.Empty;
            // Parse Message Type (We know where it starts, find the next quotation, which will be where it ends)
            for(int i = _messageTypeBegin; i < data.Length; i++) 
            {
                if (data.Span[i] == stringByte)
                {
                    messageType = data.Slice(_messageTypeBegin, (i - 1) - _messageTypeBegin).ToString();
                    break;
                }
            }

            byte leftCurlyBracket = 0x7B;
            if (messageType == "snapshot")
            {
                if(_snapshotMessageBegin == -1)
                {
                    // Find curly bracket, where the data will exist
                    for(int i = _messageTypeBegin; i < data.Length; i++)
                    {
                        if (data.Span[i] == leftCurlyBracket)
                        {
                            _snapshotMessageBegin = i + 1;
                            break;
                        }
                    }
                }

            }
            else if (messageType == "delta")
            {

            }
            else
            {
                _logger.LogTrace("Failed to parse using unsafe stream, attempting via base method");
                await base.HandleStreamMessage(type, data).ConfigureAwait(false);
                return;
            }


            // 2. Read data into accessor
            _accessor.Read(data);
            try
            {
                bool outputOriginalData = ApiClient.ApiOptions.OutputOriginalData ?? ApiClient.ClientOptions.OutputOriginalData;
                if (outputOriginalData)
                {
                    originalData = _accessor.GetOriginalString();
                    _logger.ReceivedData(SocketId, originalData);
                }

                // 3. Determine the identifying properties of this message
                var listenId = ApiClient.GetListenerIdentifier(_accessor);
                if (listenId == null)
                {
                    originalData = outputOriginalData ? _accessor.GetOriginalString() : "[OutputOriginalData is false]";
                    if (!ApiClient.UnhandledMessageExpected)
                        _logger.FailedToEvaluateMessage(SocketId, originalData);

                    UnhandledMessage?.Invoke(_accessor);
                    return;
                }

                // 4. Get the listeners interested in this message
                List<IMessageProcessor> processors;
                lock (_listenersLock)
                    processors = _listeners.Where(s => s.ListenerIdentifiers.Contains(listenId)).ToList();

                if (processors.Count == 0)
                {
                    if (!ApiClient.UnhandledMessageExpected)
                    {
                        List<string> listenerIds;
                        lock (_listenersLock)
                            listenerIds = _listeners.SelectMany(l => l.ListenerIdentifiers).ToList();
                        _logger.ReceivedMessageNotMatchedToAnyListener(SocketId, listenId, string.Join(",", listenerIds));
                        UnhandledMessage?.Invoke(_accessor);
                    }

                    return;
                }

                _logger.ProcessorMatched(SocketId, processors.Count, listenId);
                var totalUserTime = 0;
                Dictionary<Type, object>? desCache = null;
                if (processors.Count > 1)
                {
                    // Only instantiate a cache if there are multiple processors
                    desCache = new Dictionary<Type, object>();
                }

                foreach (var processor in processors)
                {
                    // 5. Determine the type to deserialize to for this processor
                    var messageType = processor.GetMessageType(_accessor);
                    if (messageType == null)
                    {
                        _logger.ReceivedMessageNotRecognized(SocketId, processor.Id);
                        continue;
                    }

                    if (processor is Subscription subscriptionProcessor && !subscriptionProcessor.Confirmed)
                    {
                        // If this message is for this listener then it is automatically confirmed, even if the subscription is not (yet) confirmed
                        subscriptionProcessor.Confirmed = true;
                        // This doesn't trigger a waiting subscribe query, should probably also somehow set the wait event for that
                    }

                    // 6. Deserialize the message
                    object? deserialized = null;
                    desCache?.TryGetValue(messageType, out deserialized);

                    if (deserialized == null)
                    {
                        var desResult = processor.Deserialize(_accessor, messageType);
                        if (!desResult)
                        {
                            _logger.FailedToDeserializeMessage(SocketId, desResult.Error?.ToString());
                            continue;
                        }
                        deserialized = desResult.Data;
                        desCache?.Add(messageType, deserialized);
                    }

                    // 7. Hand of the message to the subscription
                    try
                    {
                        var innerSw = Stopwatch.StartNew();
                        await processor.Handle(this, new DataEvent<object>(deserialized, null, null, originalData, receiveTime, null)).ConfigureAwait(false);
                        if (processor is Query query && query.RequiredResponses != 1)
                            _logger.LogDebug($"[Sckt {SocketId}] [Req {query.Id}] responses: {query.CurrentResponses}/{query.RequiredResponses}");
                        totalUserTime += (int)innerSw.ElapsedMilliseconds;
                    }
                    catch (Exception ex)
                    {
                        _logger.UserMessageProcessingFailed(SocketId, ex.ToLogString(), ex);
                        if (processor is Subscription subscription)
                            subscription.InvokeExceptionHandler(ex);
                    }
                }

                _logger.MessageProcessed(SocketId, sw.ElapsedMilliseconds, sw.ElapsedMilliseconds - totalUserTime);
            }
            finally
            {
                _accessor.Clear();
            }
        }
    }
}
