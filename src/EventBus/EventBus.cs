using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace EventBus;

public class EventBus: IEventBus
{
    private interface ITopicData
    {
        object SubjectLock { get; }
        void SetDisposed(bool isDisposed); //i'm not sure about this
        void Complete();
    }
    private class TopicData<T> : ITopicData where T : IEvent
    {
        public BlockingCollection<T> Queue { get; } = new();
        //5public object QueueLock { get; } = new(); //for monitor wait and pulse
        public Subject<T> Subject { get; } = new();
        public object SubjectLock { get; } = new();
        public volatile bool IsDisposed = false;

        public void SetDisposed(bool isDisposed)
        {
            IsDisposed = isDisposed;
        }

        public void Complete()
        {
            lock (SubjectLock)
            {
                Subject.OnCompleted();
            }
        }
    }

    private readonly ConcurrentDictionary<Type, ITopicData> _topics = new();
    //private const LazyThreadSafetyMode LazyMode = LazyThreadSafetyMode.ExecutionAndPublication;
    private readonly ConcurrentQueue<Task> _workers = [];
    private volatile int _disposed;

    public void Publish<T>(T message) where T : IEvent
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
        if (_topics.GetOrAdd(typeof(T), _ => NewTopicData<T>() ) is not TopicData<T> topicData) throw new InvalidCastException();
        topicData.Queue.Add(message);
    }
    

    private static void ProcessQueue<T>(TopicData<T> topicData) where T : IEvent
    {
        while (!topicData.Queue.IsCompleted)
        {
            T message;
            try
            {
                message = topicData.Queue.Take();
            }
            catch (InvalidOperationException) { continue; }

            topicData.Subject.OnNext(message);
            //Насколько я поняла, исключения в OnNext не должны перехватываться, для более прозрачной обработки
            //и дебага. Вообще, стоит избегать исключений в обработчиках сообщений, потому что одно исключение 
            //ведёт к неоднозначности, получат ли это сообщение другие обработчики, а также, потому что ни тот,
            //кто кидает ивенты, ни eventBus не должны знать, как их обрабатывать.
        }
    }

    public IObservable<T> GetEventStream<T>() where T : IEvent
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
        if (_topics.GetOrAdd(typeof(T), _ => NewTopicData<T>() ) is not TopicData<T> topicData) throw new InvalidCastException();
        lock (topicData.SubjectLock)
        {
            ObjectDisposedException.ThrowIf(topicData.IsDisposed, topicData);
            return topicData.Subject.AsObservable();
        }
    }

    private TopicData<T> NewTopicData<T>() where T : IEvent
    {
        var newTopicData = new TopicData<T>();
        _workers.Enqueue(Task.Run(() => ProcessQueue(newTopicData)));
        return newTopicData;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        foreach (var pair in _topics)
        {
            var topicData = pair.Value;
            lock (topicData.SubjectLock)
            {
                topicData.SetDisposed(true);
            }
        }

        var tasks = _workers.ToArray();
        Task.WaitAll(tasks);
        
        foreach (var pair in _topics)
        {
            var topicData = pair.Value;
            lock (topicData.SubjectLock)
            {
                topicData.Complete();
                //topicData.Subject.Dispose();
            }
        }
    }
}