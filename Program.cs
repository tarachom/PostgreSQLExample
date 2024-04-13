using Npgsql;

PostgreSQL postgreSQL = new();

if (postgreSQL.Open("localhost", "postgres", "1", 5432, "test") && 
    await postgreSQL.TryConnection())
{
    // Додаємо нову таблицю
    {
        await postgreSQL.ExecuteSQL($@"
CREATE TABLE IF NOT EXISTS tab1 
(
    uid serial NOT NULL,
    datewrite timestamp without time zone NOT NULL,
    info text,
    PRIMARY KEY(uid)
)");
    }

    // Додаємо індекс по полю datewrite
    {
        await postgreSQL.ExecuteSQL($@"
CREATE INDEX IF NOT EXISTS tab1_datewrite_idx ON tab1(datewrite)");
    }

    // Додаємо новий запис в таблицю
    {
        await postgreSQL.ExecuteSQL($@"
INSERT INTO tab1 (datewrite, info) 
VALUES (CURRENT_TIMESTAMP, @info_text)",
        new Dictionary<string, object>() { { "info_text", "text text text" } });
    }

    // Оновлення даних якщо дата запису менше 30 секунд
    {
        await postgreSQL.ExecuteSQL($@"
UPDATE tab1 
    SET info = @info_text 
WHERE
    datewrite < (CURRENT_TIMESTAMP::timestamp - INTERVAL '30 seconds')",
        new Dictionary<string, object>() { { "info_text", "text update" } });
    }

    // Видалення даних якщо дата запису менше 2 хвилин
    {
        await postgreSQL.ExecuteSQL($@"
DELETE FROM tab1  
WHERE
    datewrite < (CURRENT_TIMESTAMP::timestamp - INTERVAL '120 seconds')");
    }

    // Вибірка
    {
        var recordResult = await postgreSQL.SelectRequest($@"
SELECT
    uid,
    datewrite,
    info
FROM tab1
ORDER BY 
    datewrite
");

        if (recordResult.Result)
            foreach (var row in recordResult.ListRow)
            {
                string line = "";
                foreach (var column in recordResult.ColumnsName)
                    line += row[column] + " ";

                Console.WriteLine(line);
            }
    }
}
else
    Console.WriteLine("Невдалось підключитися до сервера");


class PostgreSQL
{
    NpgsqlDataSource? DataSource { get; set; }

    /// <summary>
    /// Підключення до сервера
    /// </summary>
    public bool Open(string Server, string UserId, string Password, int Port, string Database)
    {
        string conString = $"Server={Server};Username={UserId};Password={Password};Port={Port};Database={Database};SSLMode=Prefer;";

        try
        {
            NpgsqlDataSourceBuilder dataBuilder = new(conString);
            DataSource = dataBuilder.Build();

            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            return false;
        }
    }

    /// <summary>
    /// Перевірка підключення
    /// </summary>
    public async ValueTask<bool> TryConnection()
    {
        if (DataSource != null)
        {
            try
            {
                var conn = await DataSource.OpenConnectionAsync();
                await conn.CloseAsync();

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }
        else
            return false;
    }

    /// <summary>
    /// Виконання запиту який не повертає результату
    /// </summary>
    public async ValueTask<int> ExecuteSQL(string query, Dictionary<string, object>? paramQuery = null)
    {
        if (DataSource != null)
        {
            await using NpgsqlCommand command = DataSource.CreateCommand(query);

            if (paramQuery != null)
                foreach (KeyValuePair<string, object> param in paramQuery)
                    command.Parameters.AddWithValue(param.Key, param.Value);

            return await command.ExecuteNonQueryAsync();
        }
        else
            return -1;
    }

    /// <summary>
    /// Виконання запиту який повертає результати
    /// </summary>
    public async ValueTask<SelectRequestRecord> SelectRequest(string selectQuery, Dictionary<string, object>? paramQuery = null)
    {
        SelectRequestRecord record = new();

        if (DataSource != null)
        {
            NpgsqlCommand command = DataSource.CreateCommand(selectQuery);

            if (paramQuery != null)
                foreach (KeyValuePair<string, object> param in paramQuery)
                    command.Parameters.AddWithValue(param.Key, param.Value);

            NpgsqlDataReader reader = await command.ExecuteReaderAsync();

            record.Result = reader.HasRows;

            int columnsCount = reader.FieldCount;
            record.ColumnsName = new string[columnsCount];

            for (int n = 0; n < columnsCount; n++)
                record.ColumnsName[n] = reader.GetName(n);

            while (await reader.ReadAsync())
            {
                Dictionary<string, object> objRow = [];

                for (int i = 0; i < columnsCount; i++)
                    objRow.Add(record.ColumnsName[i], reader[i]);

                record.ListRow.Add(objRow);
            }
            await reader.CloseAsync();
        }

        return record;
    }
}

/// <summary>
/// Структура для повернення результату з функції SelectRequest
/// </summary>
public record SelectRequestRecord
{
    /// <summary>
    /// Результат функції
    /// </summary>
    public bool Result;

    /// <summary>
    /// Колонки
    /// </summary>
    public string[] ColumnsName = [];

    /// <summary>
    /// Список рядків
    /// </summary>
    public List<Dictionary<string, object>> ListRow = [];
}

