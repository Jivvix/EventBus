// Edit file just to make PR to leave comments
using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace EventBus;

public class EventBus<T> : IEventBus<T>
{
    private class TopicData
    {
        public ConcurrentQueue<T> Queue { get; } = new();
        public object QueueLock { get; } = new(); //for monitor wait and pulse
        public Subject<T> Subject { get; } = new();
        public object SubjectLock { get; } = new();
        public volatile bool IsDisposed;
    }

    private readonly ConcurrentDictionary<string, Lazy<TopicData>> _topics = new();
    private const LazyThreadSafetyMode LazyMode = LazyThreadSafetyMode.ExecutionAndPublication;
    private readonly List<Task> _workers = [];
    private readonly Lock _workerLock = new();
    private volatile int _disposed;

    public void Publish(T message, string topic = "default")
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
        var topicData = _topics.GetOrAdd(topic, _ => NewTopicData() ).Value;
        topicData.Queue.Enqueue(message);
        lock (topicData.QueueLock)
        {
            Monitor.Pulse(topicData.QueueLock);
        }
    }
    

    private static void ProcessQueue(TopicData topicData)
    {
        while (!topicData.IsDisposed)
        {
            while (topicData.Queue.IsEmpty && !topicData.IsDisposed)
            {
                lock (topicData.QueueLock)
                {
                    Monitor.Wait(topicData.QueueLock);
                }
                if (topicData.IsDisposed) return;
            }

            while (topicData.Queue.TryDequeue(out var message))
            {
                lock (topicData.SubjectLock)
                {
                    if (topicData.IsDisposed || topicData.Subject.IsDisposed) return;
                    topicData.Subject.OnNext(message);
                    //Насколько я поняла, исключения в OnNext не должны перехватываться, для более прозрачной обработки
                    //и дебага. Вообще, стоит избегать исключений в обработчиках сообщений, потому что одно исключение 
                    //ведёт к неоднозначности, получат ли это сообщение другие обработчики, а также, потому что ни тот,
                    //кто кидает ивенты, ни eventBus не должны знать, как их обрабатывать.
                }
            }
        }
    }

    public IObservable<T> GetEventStream(string topic = "default")
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
        var topicData = _topics.GetOrAdd(topic, _ => NewTopicData()).Value;
        lock (topicData.SubjectLock)
        {
            ObjectDisposedException.ThrowIf(topicData.IsDisposed, topicData);
            return topicData.Subject.AsObservable();
        }
    }

    private Lazy<TopicData> NewTopicData()
    {
        return new Lazy<TopicData>(() =>
            {
                var newTopicData = new TopicData();
                lock (_workerLock)
                {
                    _workers.Add(Task.Run(() => ProcessQueue(newTopicData)));
                }
                return newTopicData;
            }, 
            LazyMode);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        foreach (var pair in _topics)
        {
            var topicData = pair.Value.Value;
            lock (topicData.SubjectLock)
            {
                topicData.IsDisposed = true;
            }
            lock (topicData.QueueLock)
            {
                Monitor.PulseAll(topicData.QueueLock);
            }
        }

        Task[] tasks;
        lock (_workerLock)
        {
            tasks = _workers.ToArray();
        }
        Task.WaitAll(tasks);
        
        foreach (var pair in _topics)
        {
            var topicData = pair.Value.Value;
            lock (topicData.SubjectLock)
            {
                topicData.Subject.OnCompleted();
                topicData.Subject.Dispose();
            }
        }
    }
}
