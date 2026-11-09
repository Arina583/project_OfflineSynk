import java.sql.Connection
import java.sql.DriverManager

object Sync {
    const val BATCH_SIZE = 5          // размер батча
    const val BASE_DELAY_MS = 1000L   // база экспоненциального backoff

    var online = true                 // флаг сети; false имитирует разрыв связи
    private var clock = System.currentTimeMillis()

    /** Монотонные метки: даже в пределах одной миллисекунды значения строго растут. */
    fun now(): Long {
        clock = maxOf(clock + 1, System.currentTimeMillis())
        return clock
    }

    val CLIENT_SCHEMA = """
        CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, value TEXT NOT NULL,
            local_updated_at INTEGER NOT NULL, synced INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS outbox (
            mutation_id TEXT PRIMARY KEY, op_type TEXT NOT NULL, entity_id TEXT NOT NULL,
            title TEXT NOT NULL, value TEXT NOT NULL,
            created_at INTEGER NOT NULL, local_updated_at INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, next_retry_at INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS idx_outbox_retry ON outbox(next_retry_at);
    """.trimIndent()

    val SERVER_SCHEMA = """
        CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, value TEXT NOT NULL,
            updated_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS processed (mutation_id TEXT PRIMARY KEY);
    """.trimIndent()

    fun client(file: String = "client.db"): Connection = open(file, CLIENT_SCHEMA)
    fun server(file: String = "server.db"): Connection = open(file, SERVER_SCHEMA)

    private fun open(file: String, schema: String): Connection =
        DriverManager.getConnection("jdbc:sqlite:$file").apply {
            schema.split(";").map { it.trim() }.filter { it.isNotEmpty() }
                .forEach { createStatement().executeUpdate(it) }
        }

    data class Op(
        val mutationId: String, val entityId: String,
        val title: String, val value: String,
        val createdAt: Long, val localUpdatedAt: Long, val attempts: Int
    )

    /** Локальная запись + элемент очереди — ОДНА транзакция. */
    fun addNote(conn: Connection, id: String, title: String, value: String) {
        val ts = now()
        conn.autoCommit = false
        try {
            conn.prepareStatement("INSERT OR REPLACE INTO notes VALUES (?, ?, ?, ?, 0)").use {
                it.setString(1, id); it.setString(2, title)
                it.setString(3, value); it.setLong(4, ts); it.executeUpdate()
            }
            conn.prepareStatement(
                """INSERT OR REPLACE INTO outbox (mutation_id, op_type, entity_id,
                   title, value, created_at, local_updated_at, attempts, next_retry_at)
                   VALUES (?, 'CREATE', ?, ?, ?, ?, ?, 0, 0)""").use {
                it.setString(1, "$id:$ts"); it.setString(2, id)
                it.setString(3, title); it.setString(4, value)
                it.setLong(5, ts); it.setLong(6, ts); it.executeUpdate()
            }
            conn.commit()
        } catch (e: Exception) { conn.rollback(); throw e } finally { conn.autoCommit = true }
    }

    private fun nextBatch(conn: Connection, size: Int): List<Op> =
        conn.prepareStatement(
            """SELECT mutation_id, entity_id, title, value, created_at,
               local_updated_at, attempts FROM outbox
               WHERE next_retry_at <= ? ORDER BY created_at LIMIT ?""").use { st ->
            st.setLong(1, now()); st.setInt(2, size)
            st.executeQuery().use { rs ->
                buildList {
                    while (rs.next()) add(Op(rs.getString(1), rs.getString(2),
                        rs.getString(3), rs.getString(4), rs.getLong(5), rs.getLong(6), rs.getInt(7)))
                }
            }
        }

    /** Применение операции на «сервере»: идемпотентность + LWW. */
    private fun serverApply(srv: Connection, op: Op): String {
        val dup = srv.prepareStatement("SELECT 1 FROM processed WHERE mutation_id = ?").use {
            it.setString(1, op.mutationId); it.executeQuery().next()
        }
        if (dup) return "duplicate"
        srv.prepareStatement("INSERT INTO processed VALUES (?)").use {
            it.setString(1, op.mutationId); it.executeUpdate()
        }
        val existing = srv.prepareStatement("SELECT updated_at FROM notes WHERE id = ?").use {
            it.setString(1, op.entityId)
            it.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else null }
        }
        if (existing != null && existing >= op.localUpdatedAt) return "rejected_lww"
        srv.prepareStatement("INSERT OR REPLACE INTO notes VALUES (?, ?, ?, ?)").use {
            it.setString(1, op.entityId); it.setString(2, op.title)
            it.setString(3, op.value); it.setLong(4, op.localUpdatedAt); it.executeUpdate()
        }
        return "accepted"
    }

    /** Пакетная отправка очереди. Возвращает число отправленных операций. */
    fun push(conn: Connection): Int {
        var sent = 0
        server().use { srv ->
            while (true) {
                val batch = nextBatch(conn, BATCH_SIZE)
                if (batch.isEmpty()) break
                if (!online) {                        // временная ошибка: backoff
                    for (op in batch) {
                        val a = op.attempts + 1
                        conn.prepareStatement(
                            "UPDATE outbox SET attempts = ?, next_retry_at = ? WHERE mutation_id = ?").use {
                            it.setInt(1, a)
                            it.setLong(2, now() + BASE_DELAY_MS * (1L shl a))
                            it.setString(3, op.mutationId); it.executeUpdate()
                        }
                    }
                    println("нет связи: батч из ${batch.size} операций отложен (backoff)")
                    break
                }
                srv.autoCommit = false                // сначала применяем на сервере...
                for (op in batch) serverApply(srv, op)
                srv.commit()
                for (op in batch) {                   // ...и только потом снимаем с очереди
                    conn.prepareStatement("DELETE FROM outbox WHERE mutation_id = ?").use {
                        it.setString(1, op.mutationId); it.executeUpdate()
                    }
                    conn.prepareStatement("UPDATE notes SET synced = 1 WHERE id = ?").use {
                        it.setString(1, op.entityId); it.executeUpdate()
                    }
                }
                sent += batch.size
                println("батч отправлен: ${batch.size} операций")
            }
        }
        return sent
    }

    fun pendingCount(conn: Connection): Int =
        conn.createStatement().use {
            it.executeQuery("SELECT COUNT(*) FROM outbox").use { rs ->
                rs.next(); rs.getInt(1)
            }
        }
}
