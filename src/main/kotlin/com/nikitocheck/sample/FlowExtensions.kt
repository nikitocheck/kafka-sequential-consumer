import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

fun <T : Any> Flow<T>.sendToChannel(channel: Channel<T>): Flow<T> {
    return map {
        println("Consumed message=$it. Sending to channel...")
        channel.send(it)
        it
    }
}