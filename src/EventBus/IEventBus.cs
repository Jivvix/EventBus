namespace EventBus;

/// <summary>
/// Передаёт ивенты (классы, наследующиеся от IEvent). Отдельная подписка и отдельная очередь для каждого типа ивентов.
/// Чтобы получать разные классы ивентов из одной очереди, нужно их отнаследовать от общего родителя и указывать его тип
/// в методах.
/// </summary>
public interface IEventBus : IDisposable
{
    /// <summary>
    /// Публикует сообщение соответствующегоо типа.
    /// </summary>
    /// <param name="message">Сообщение для публикации.</param>
    /// <typeparam name="T">Тип сообщения для передачи</typeparam>
    /// <exception cref="ObjectDisposedException"> Если EventBroker is disposed</exception>
    void Publish<T>(T message)  where T : IEvent;


    /// <summary>
    /// Возвращает поток событий указанного типа.
    /// Избегайте бросать исключений в подписчиках, так как это ведёт к неопределённости, кто их будет ловить и
    /// обрабатывать.
    /// </summary>
    /// <typeparam name="T">Тип получаемых сообщений</typeparam>
    /// <returns>IObservable поток сообщений типа T.</returns>
    /// <exception cref="ObjectDisposedException">Если EventBroker is disposed или топик is disposed</exception>
    IObservable<T> GetEventStream<T>()  where T : IEvent;
}