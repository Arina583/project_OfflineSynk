fun main() {
    // Этап 1: связь есть, три заметки уходят сразу
    Sync.online = true
    val c = Sync.client()
    repeat(3) { Sync.addNote(c, "n$it", "Заметка $it", "значение $it") }
    Sync.push(c)
    println("Этап 1. В очереди: ${Sync.pendingCount(c)}")               // 0

    // Этап 2: связь потеряна, 12 заметок копятся в очереди
    Sync.online = false
    for (i in 3..14) Sync.addNote(c, "n$i", "Заметка $i", "значение $i")
    Sync.push(c)                                                        // уходит в backoff
    println("Этап 2. В очереди: ${Sync.pendingCount(c)}")               // 12

    // Этап 3: связь восстановилась — сбрасываем паузу backoff
    c.createStatement().use {
        it.executeUpdate("UPDATE outbox SET next_retry_at = 0, attempts = 0") }
    Sync.online = true
    val sent = Sync.push(c)
    println("Этап 3. Отправлено: $sent, осталось: ${Sync.pendingCount(c)}") // 12, 0
    // в консоли: «батч отправлен: 5», «5», «2» — ровно три батча

    // Этап 4: конфликт двух клиентов (LWW)
    Sync.online = false
    val c1 = Sync.client("client1.db")
    val c2 = Sync.client("client2.db")
    Sync.addNote(c1, "shared", "Объект", "правка клиента A")   // метка T1
    Sync.addNote(c2, "shared", "Объект", "правка клиента B")   // T2 > T1 — часы монотонные
    Sync.online = true
    Sync.push(c2)   // B доезжает первым и принимается
    Sync.push(c1)   // A старее — сервер отклоняет по LWW
    Sync.server().use { s ->
        s.createStatement().use {
            it.executeQuery("SELECT value FROM notes WHERE id = 'shared'").use { rs ->
                rs.next()
                println("Этап 4. Победившая версия на сервере: ${rs.getString(1)}")
            }
        }
    }
}