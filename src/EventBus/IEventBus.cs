namespace EventBus;

public interface IEventBus : IDisposable
{
    /// <summary>
    /// Публикует сообщение в указанный топик. Если топик не был создан ранее, то создаёт его.
    /// </summary>
    /// <param name="message">Сообщение для публикации.</param>
    /// <param name="topic">Имя топика (канала). Если не указывать, то публикуется в топик по умолчанию</param>
    /// <exception cref="ObjectDisposedException"> Если EventBroker is disposed</exception>
    void Publish<T>(T message)  where T : IEvent;


    /// <summary>
    /// Возвращает поток событий для указанного топика. Если топик не был создан ранее, то создаёт его.
    /// Избегайте бросания исключений в подписчиках, так как это ведёт к неопределённости, кто их будет ловить и
    /// обрабатывать.
    /// </summary>
    /// <param name="topic">Имя топика. Если не указано, то используется топик по умолчанию.</param>
    /// <returns>IObservable поток сообщений типа T.</returns>
    /// <exception cref="ObjectDisposedException">Если EventBroker is disposed или топик is disposed</exception>
    IObservable<T> GetEventStream<T>()  where T : IEvent;
}