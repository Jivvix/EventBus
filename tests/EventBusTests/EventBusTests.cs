using EventBus;

namespace EventBusTests;



[TestFixture]
public class EventBusTests
{
    private class SimpleEvent : IEvent {}

    private class StringEvent(string text) : IEvent
    {
        public string Text { get; set; } = text;
    }
    private IEventBus _eventBus;

    [SetUp]
    public void Setup()
    {
        _eventBus = new EventBus.EventBus();
    }

    [TearDown]
    public void TearDown()
    {
        _eventBus.Dispose();
    }

    [Test]
    public void Publish_ShouldDeliverMessageToSubscriber()
    {
       
        int receivedMessageCount = 0;
        using var subscription = _eventBus.GetEventStream<SimpleEvent>().Subscribe(m => receivedMessageCount++);
        var simpleEvent = new SimpleEvent();
        _eventBus.Publish(simpleEvent);

        Thread.Sleep(100);
        Assert.That(receivedMessageCount, Is.EqualTo(1));
    }

    [Test]
    public void GetEventStream_ShouldReturnObservableForRequestedType()
    {
        var observable = _eventBus.GetEventStream<SimpleEvent>();

        Assert.That(observable, Is.Not.Null);
        Assert.That(observable, Is.InstanceOf<IObservable<SimpleEvent>>());
    }

    [Test]
    public void Publish_ShouldNotDeliverMessageToDifferentTypeSubscribers()
    {
        var stringEvent = new StringEvent("Test string");
        var receivedStrings = new List<string>();
        var receivedSimpleEvent = 0;

        using var sub1 = _eventBus.GetEventStream<SimpleEvent>().Subscribe(m => receivedSimpleEvent++);
        using var sub2 = _eventBus.GetEventStream<StringEvent>().Subscribe(m => receivedStrings.Add(m.Text));

        _eventBus.Publish(stringEvent);

        Thread.Sleep(100);
        Assert.That(receivedSimpleEvent, Is.EqualTo(0));
        Assert.That(receivedStrings.Count, Is.EqualTo(1));
        Assert.That(receivedStrings[0], Is.EqualTo("Test string"));
    }

    [Test]
    public void Dispose_ShouldCompleteSubscriber()
    {
        var eventBus = new EventBus.EventBus();
        var isCompleted = false;
        using var subscription = eventBus.GetEventStream<SimpleEvent>()
            .Subscribe(_ => { }, () => isCompleted = true);
        eventBus.Dispose();
        Assert.That(isCompleted, Is.True);
    }

    [Test]
    public void Publish_AfterDispose_ShouldThrowObjectDisposedException()
    {
        var eventBus = new EventBus.EventBus();
        eventBus.Dispose();
        var simpleEvent = new SimpleEvent();
        Assert.Throws<ObjectDisposedException>(() => eventBus.Publish(simpleEvent));
    }

    [Test]
    public void GetEventStream_AfterDispose_ShouldThrowObjectDisposedException()
    {
        var eventBus = new EventBus.EventBus();
        eventBus.Dispose();

        Assert.Throws<ObjectDisposedException>(() => eventBus.GetEventStream<SimpleEvent>());
    }

    [Test]
    public void MultipleSubscribers_ShouldAllReceiveMessages()
    {
        var stringEvent = new StringEvent("Test string");
        var receivedCount = 0;
        using var sub1 = _eventBus.GetEventStream<StringEvent>().Subscribe(_ => Interlocked.Increment(ref receivedCount));
        using var sub2 = _eventBus.GetEventStream<StringEvent>().Subscribe(_ => Interlocked.Increment(ref receivedCount));

        _eventBus.Publish(stringEvent);

        Thread.Sleep(100);
        Assert.That(receivedCount, Is.EqualTo(2));

    }

    [Test]
    public async Task HighLoad_ShouldHandleMultipleMessages()
    {
        const int messageCount = 1000;
        var receivedMessages = new List<string>();
        using var subscription = _eventBus.GetEventStream<StringEvent>().Subscribe(msg => receivedMessages.Add(msg.Text));
        
        Parallel.For(0, messageCount, i => _eventBus.Publish(new StringEvent($"Message {i}")) );

        await Task.Delay(500);
        var expected = Enumerable.Range(0, messageCount)
            .Select(i => $"Message {i}");
        Assert.That(receivedMessages, Has.Count.EqualTo(messageCount));
        Assert.That(receivedMessages, Is.EquivalentTo(expected));
    }
}