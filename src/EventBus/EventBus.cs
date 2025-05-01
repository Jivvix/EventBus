using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace EventBus;

// Вроде бы, если <T> указывать в классе, то сообщение будет одного и того же типа для всех топиков. не знаю, можно ли так, но по идее если <T> указать на уровне метода, а не класса-интерфейса, то внутри одного EventBus'а можно будет реально разные типы складывать. В моём представлении тогда метод должен выглядеть, как public void Public<T>(T message, ...)
// UPD: Хотя тогда будет непонятно что возвращать GetEventStream. В таком случае могу предложить оставить <T> на уровне класса, но добавить Publish<E : T> (ну или как тут наследование дженериков работает, если оно вообще есть). Насколько я понял, сейчас ты можешь отправлять ивенты только одного типа. В тестах ты посылаешь только строки, но надо бы проверить и другие типы данных.
// В тестах можно проверить посылку строк и целых чисел, но тогда T будет просто равен Object, как общему объекту. Если хочется тестов по-серьёзнее, наверное стоит сделать класс Event, отнаследовать от него несколько ивентов, и попробовать послать каждый из них.
// UPD 2: TopicData при таком подходе, по идее, тоже будет иметь <E : T>, и внутри себя использовать уже E
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
// Думаю, вместо лока на воркеров, лучше использовать ConcurrentList (ну или что там есть)
    private readonly List<Task> _workers = [];
    private readonly Lock _workerLock = new();
// А почему _disposed не bool? 
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
// Действительно фигня какая-то с блокировкой очереди получается. Попробуй посмотреть на BlockingCollection, кажется, эта штука тебе больше подойдёт. BlockingCollection имеет метод Take который заблокируется, пока в коллекции не появятся элементы. Засчёт этого у тебя и цикл не будет крутиться вхолостую, и лишних локов быть не должно, да и TryDequeue можно избежать. Вероятно, придётся отловить исключения на случай dispose, но ты всё равно посмотри.
// UPD: В BlockingCollection есть метод CompleteAdding, который можно вызывать в Dispose. Надо посмотреть, как этим пользоваться
                lock (topicData.QueueLock)
                {
                    Monitor.Wait(topicData.QueueLock);
                }
                if (topicData.IsDisposed) return;
            }

            while (topicData.Queue.TryDequeue(out var message))
            {
// Чёт есть у меня подозрение, что не должно быть здесь лока. Думаю, можно вызывать просто OnNext, а потом обрабатывать исключения. В смысле это довольно редкий и какой-то некорректный сценарий, когда ты пытаешься обработать сообщение, а у тебя одновременно с этим закрывают брокер. На этот случай, как будто, и не стыдно потратиться на исключение, чтобы в остальных случаях без лочек всё быстрее работало.
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
// Не-е, нефига, Lazy здесь не поможет, как я понял. Lazy используется для ресурсоёмких объектов, и гарантирует, что обращение к одному и тому же лейзи из разных потоков создаст только один объект. Здесь проблема в том, что сама функция может вызваться дважды, соответственно создастся два разных Lazy-объекта на один и тот же топик. Здесь, вероятно, придётся использовать lock опять.
// Предлагаю идею, как я тебе рассказывал, через двойную проверку. Первая проверка осуществляет сама GetOrAdd, так что её прописывать не надо, а вот вторую проверку нужно осуществлять под локом. В локе на словарь проверить наличие ключа, если его нет - создать, если он есть - вернуть.
// По идее этот метод имеет смысл использовать вместе с ConcurrentMap как раз из-за того, что этот лок происходит нечасто, только при создании топиков.
// Хотя по мне оч странно, что сама ConcurrentMap, хоть и заточена под потокобезопасность, позволяетсоздавать два объекта под ключами. Какие-то они идиоты.
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
