using Npgsql;

const string host = "localhost";
const string user = "postgres";
const string password = "1";
const int port = 5432;
const string database = "test5";

PostgreSQL postgreSQL = new();

//Створення бази даних якщо така база ще не створена
if (!await postgreSQL.CreateDatabaseIfNotExist(host, user, password, port, database))
{
    Console.WriteLine("Невдалось створити базу даних");
    return;
}

//Підключення до бази даних
if (postgreSQL.Open(host, user, password, port, database) &&
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
        var param = new Dictionary<string, object>() { { "info_text", "text text text" } };

        await postgreSQL.ExecuteSQL($@"
INSERT INTO tab1 (datewrite, info) 
VALUES (CURRENT_TIMESTAMP, @info_text)", param);
    }

    // Оновлення даних якщо дата запису менше 30 секунд
    {
        var param = new Dictionary<string, object>() { { "info_text", "text update" } };

        await postgreSQL.ExecuteSQL($@"
UPDATE tab1 
    SET info = @info_text 
WHERE
    datewrite < (CURRENT_TIMESTAMP::timestamp - INTERVAL '30 seconds')", param);
    }

    // Видалення даних якщо дата запису менше 2 хвилин
    {
        await postgreSQL.ExecuteSQL($@"
DELETE FROM tab1  
WHERE
    datewrite < (CURRENT_TIMESTAMP::timestamp - INTERVAL '2 minute')");
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
                    line += row[column].ToString() + " ";

                Console.WriteLine(line);
            }
    }
}
else
    Console.WriteLine("Невдалось підключитися до сервера");

Console.ReadLine();

class PostgreSQL
{
    NpgsqlDataSource? DataSource { get; set; }

    // Підключення до сервера
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

    // Перевірка підключення
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

    //Створення бази
    public async ValueTask<bool> CreateDatabaseIfNotExist(string Server, string UserId, string Password, int Port, string Database)
    {
        string conString = $"Server={Server};Username={UserId};Password={Password};Port={Port};SSLMode=Prefer;";

        try
        {
            NpgsqlDataSourceBuilder dataBuilder = new(conString);
            DataSource = dataBuilder.Build();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            return false;
        }

        if (DataSource != null)
        {
            if (!await IfExistDatabase(Database))
            {
                NpgsqlCommand command = DataSource.CreateCommand($"CREATE DATABASE {Database}");

                try
                {
                    await command.ExecuteNonQueryAsync();
                    return true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return false;
                }
            }
            else
                return true;
        }
        else
            return false;
    }

    //Перевірка наявності бази
    async ValueTask<bool> IfExistDatabase(string Database)
    {
        if (DataSource != null)
        {
            string sql = @"
SELECT EXISTS
(
    SELECT 
        datname 
    FROM 
        pg_catalog.pg_database 
    WHERE 
        lower(datname) = lower(@databasename)
)";

            NpgsqlCommand command = DataSource.CreateCommand(sql);
            command.Parameters.AddWithValue("databasename", Database);

            try
            {
                object? result = await command.ExecuteScalarAsync();
                return bool.Parse(result?.ToString() ?? "false");
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

    // Виконання запиту який не повертає результату
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

    // Виконання запиту який повертає результати
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

// Структура для повернення результату з функції SelectRequest
public record SelectRequestRecord
{
    // Результат функції
    public bool Result;

    // Колонки
    public string[] ColumnsName = [];

    // Список рядків
    public List<Dictionary<string, object>> ListRow = [];
}