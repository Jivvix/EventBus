using EventBus;

namespace EventBusTests;

[TestFixture]
public class EventBusTests
{
    private IEventBus<string> _eventBus;

    [SetUp]
    public void Setup()
    {
        _eventBus = new EventBus<string>();
    }

    [TearDown]
    public void TearDown()
    {
        _eventBus.Dispose();
    }

    [Test]
    public void Publish_ShouldDeliverMessageToSubscriber_DefaultTopic()
    {
        const string message = "Test message";
        var receivedMessage = string.Empty;
        using var subscription = _eventBus.GetEventStream().Subscribe(m => receivedMessage = m);

        _eventBus.Publish(message);

        Thread.Sleep(100);
        Assert.That(receivedMessage, Is.EqualTo(message));
    }

    [Test]
    public void Publish_ShouldDeliverMessageToSubscriber_CustomTopic()
    {
        const string message = "Test message";
        const string topic = "custom-topic";
        var receivedMessage = string.Empty;
        using var subscription = _eventBus.GetEventStream(topic).Subscribe(m => receivedMessage = m);

        _eventBus.Publish(message, topic);

        Thread.Sleep(100);
        Assert.That(receivedMessage, Is.EqualTo(message));
    }

    [Test]
    public void GetEventStream_ShouldReturnObservableForRequestedTopic()
    {
        const string topic = "test-topic";

        var observable = _eventBus.GetEventStream(topic);

        Assert.That(observable, Is.Not.Null);
        Assert.That(observable, Is.InstanceOf<IObservable<string>>());
    }

    [Test]
    public void Publish_ShouldNotDeliverMessageToDifferentTopicSubscribers()
    {
        const string message = "Test message";
        const string topic1 = "topic1";
        const string topic2 = "topic2";
        var receivedMessages = new List<string>();

        using var sub1 = _eventBus.GetEventStream(topic1).Subscribe(m => receivedMessages.Add("topic1:" + m));
        using var sub2 = _eventBus.GetEventStream(topic2).Subscribe(m => receivedMessages.Add("topic2:" + m));

        _eventBus.Publish(message, topic1);

        Thread.Sleep(100);
        Assert.That(receivedMessages.Count, Is.EqualTo(1));
        Assert.That(receivedMessages[0], Is.EqualTo("topic1:" + message));
    }

    [Test]
    public void Dispose_ShouldCompleteSubscriber()
    {
        var eventBus = new EventBus<string>();
        var isCompleted = false;
        using var subscription = eventBus.GetEventStream()
            .Subscribe(_ => { }, () => isCompleted = true);
        eventBus.Dispose();

        Assert.That(isCompleted, Is.True);
    }

    [Test]
    public void Publish_AfterDispose_ShouldThrowObjectDisposedException()
    {
        var eventBus = new EventBus<string>();
        eventBus.Dispose();

        Assert.Throws<ObjectDisposedException>(() => eventBus.Publish("test"));
    }

    [Test]
    public void GetEventStream_AfterDispose_ShouldThrowObjectDisposedException()
    {
        var eventBus = new EventBus<string>();
        eventBus.Dispose();

        Assert.Throws<ObjectDisposedException>(() => eventBus.GetEventStream());
    }

    [Test]
    public void MultipleSubscribers_ShouldAllReceiveMessages()
    {
        const string message = "Test message";
        var receivedCount = 0;
        using var sub1 = _eventBus.GetEventStream().Subscribe(_ => Interlocked.Increment(ref receivedCount));
        using var sub2 = _eventBus.GetEventStream().Subscribe(_ => Interlocked.Increment(ref receivedCount));

        _eventBus.Publish(message);

        Thread.Sleep(100);
        Assert.That(receivedCount, Is.EqualTo(2));

    }

    [Test]
    public async Task HighLoad_ShouldHandleMultipleMessages()
    {
        const int messageCount = 1000;
        var receivedMessages = new List<string>();
        using var subscription = _eventBus.GetEventStream().Subscribe(msg => receivedMessages.Add(msg));
        
        Parallel.For(0, messageCount, i => _eventBus.Publish($"Message {i}"));

        await Task.Delay(500);
        var expected = Enumerable.Range(0, messageCount)
            .Select(i => $"Message {i}");
        Assert.That(receivedMessages, Has.Count.EqualTo(messageCount));
        Assert.That(receivedMessages, Is.EquivalentTo(expected));
    }
}