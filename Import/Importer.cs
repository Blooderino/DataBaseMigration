using System;
using System.Net;
using System.Net.Sockets;
using System.Xml;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Npgsql;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Grpc.Core;

namespace DataBaseMigration
{
    class Importer : Greeter.GreeterBase
    {
        // Раздилитель строк в сообщении
        private const Char MESSAGE_SEPARATOR = '\t';

        // Аргумент запуска с помощью веб-сокетов
        private const String RUN_TCP_SOCKET = "--run-tcp-socket";

        // Аргумент запуска с помощью очереди сообщений
        private const String RUN_MESSAGE_QUEUE = "--run-message-queue";

        // Аргумент запуска с помощью вызова удаленных процедур
        private const String RUN_REMOTE_PROCEDURE_CALL = "--run-remote-procedure-call";

        // Ключ RSA 2048
        private static RSACryptoServiceProvider rsa;

        // Подключение к базе данных PostgreSQL
        private static NpgsqlConnection connection = null;

        // Команды БД
        private static NpgsqlCommand command = null;

        // Текстовое сообщение с доступными аргументами запуска
        private const String MESSAGE_AVAILABLE_ARGS =
            "Доступные аргументы запуска:\n" +
            "1. Передача данных через сокеты: " + RUN_TCP_SOCKET + "\n" +
            "2. Передача данных через очередь сообщений: " + RUN_MESSAGE_QUEUE + "\n" +
            "3. Передача данных через вызов удаленных процедур: " + RUN_REMOTE_PROCEDURE_CALL + "\n" +
            "Одновременно можно использовать только один аргумент запуска.";

        // Путь до конфигурационного файла
        private const String CONFIG_PATH = "config.xml";

        // Параметр порта текущего сервера из конфигурационного файла
        private const String CONFIG_SOCKET_PORT = "/import/socket/port";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_ADDRESS = "/import/message-queue/address";

        // Параметр порта сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_PORT = "/import/message-queue/port";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_USERNAME = "/import/message-queue/username";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_PASSWORD = "/import/message-queue/password";

        // Параметр имени очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_CHANNEL = "/import/message-queue/channel";

        // Параметр порта сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_REMOTE_PROCEDURE_CALL_PORT = "/import/remote-procedure-call/port";

        // Параметр адреса базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_ADDRESS = "/import/database/address";

        // Параметр порта базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_PORT = "/import/database/port";

        // Параметр имения пользователя базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_USERNAME = "/import/database/username";

        // Параметр пароля пользователя базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_PASSWORD = "/import/database/password";

        // Параметр имени базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_NAME = "/import/database/name";

        // Текстовое сообщение с доступным форматом файла
        private const String MESSAGE_AVAILABLE_CONFIG =
            "Доступный формат конфигурационного файла:\n" +
            "<import>\n" +
            "   <socket>\n" +
            "       <port>Порт текущего сервера сокетов</port>\n" +
            "   </socket>\n" +
            "   <message-queue>\n" +
            "       <address>IP или домен сервера очереди сообщений</address>\n" +
            "       <port>Порт сервера очереди сообщений</port>\n" +
            "       <username>Имя пользователя очереди сообщений</username>\n" +
            "       <password>Пароль пользователя очереди сообщений</password>\n" +
            "       <channel>Имя очереди сообщений</channel>" +
            "   </message-queue>\n" +
            "   <remote-procedure-call>\n" +
            "       <port>Порт сервера удаленного вызова процедур</port>\n" +
            "   </remote-procedure-call>\n" +
            "   <database>" +
            "       <address>IP или домен сервера БД</address>\n" +
            "       <port>Порт сервера БД</port>\n" +
            "       <username>Имя пользователя БД</username>\n" +
            "       <password>Пароль пользователя БД</password>\n" +
            "       <name>Имя БД</name>\n" +
            "   </database>\n" +
            "</import>";

        // Конфигурационный файл в формате XML-документа
        private static XmlDocument config = null;

        // Открытый ключ RSA 2048 отправлен
        private static bool rsaPublicKeySended = false;

        // Создатель подключения gRPC
        private static IHost hostBuilder;

        // Главный метод программы
        static void Main(string[] args)
        {
            Console.WriteLine("Запуск процесса импорта данных.");

            // Если был встречен ровно 1 аргумент запуска, то начинается чтение параметров из конфигурационного файла
            if (args.Length == 1)
            {
                // Получение параметров из конфигурационного файла
                String
                    socketPort = GetConfigParam(CONFIG_PATH, CONFIG_SOCKET_PORT),
                    messageQueueAddress = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_ADDRESS),
                    messageQueuePort = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_PORT),
                    messageQueueUsername = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_USERNAME),
                    messageQueuePassword = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_PASSWORD),
                    messageQueueChannel = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_CHANNEL),
                    dbHost = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_ADDRESS),
                    dbPort = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_PORT),
                    dbUsername = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_USERNAME),
                    dbPassword = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_PASSWORD),
                    dbName = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_NAME),
                    rpcPort = GetConfigParam(CONFIG_PATH, CONFIG_REMOTE_PROCEDURE_CALL_PORT);

                // Если параметры не пустые, то переходим к попытке подключиться к БД
                if (socketPort != null &&
                    messageQueueAddress != null && messageQueuePort != null &&
                    messageQueueUsername != null && messageQueuePassword != null &&
                    messageQueueChannel != null &&
                    dbHost != null && dbPort != null && dbUsername != null && dbPassword != null && dbName != null &&
                    rpcPort != null)
                {
                    Console.WriteLine(
                    "Параметры подключения:\n" +
                    "Адрес БД: " + dbHost + ":" + dbPort + "\n" +
                    "Логин/пароль: " + dbUsername + " " + dbPassword + "\n" +
                    "Имя БД: " + dbName);

                    // Подключение к БД PostgreSQL
                    try
                    {
                        connection = new NpgsqlConnection(
                            "Host=" + dbHost + ";" +
                            "Port=" + dbPort + ";" +
                            "Username=" + dbUsername + ";" +
                            "Password=" + dbPassword + ";" +
                            "Database=" + dbName);

                        connection.Open();
                    }
                    // Если не удалось подключиться, то параметры подключения очищаются
                    catch (NpgsqlException exception) 
                    {
                        connection = null;
                    }

                    // Если параметры подключения к БД не пусты, то переходим к выбору типа передачи данных
                    if (connection != null)
                    {
                        // Подговка к выполнению команд БД
                        command = new NpgsqlCommand();
                        command.Connection = connection;

                        // Запуск передачи данных через TCP-сокет
                        if (args[0].Equals(RUN_TCP_SOCKET))
                        {
                            Console.WriteLine("Порт сервера сокетов: " + socketPort);

                            RunTCPSocket(socketPort);
                        }

                        // Запуск передачи данных через очередь сообщений
                        else if (args[0].Equals(RUN_MESSAGE_QUEUE))
                        {
                            Console.WriteLine(
                            "Адрес сервера очереди сообщений: " +
                            messageQueueAddress + ":" + messageQueuePort);

                            Console.WriteLine("Логин/пароль: " + messageQueueUsername + " " + messageQueuePassword);
                            Console.WriteLine("Имя очереди сообщений: " + messageQueueChannel);

                            RunMessageQueue(
                                messageQueueAddress, messageQueuePort,
                                messageQueueUsername, messageQueuePassword,
                                messageQueueChannel);
                        }

                        // Запуск передачи данных через вызов удаленных процедур
                        else if (args[0].Equals(RUN_REMOTE_PROCEDURE_CALL))
                        {
                            Console.WriteLine("Порт сервера удаленного вызова процедур: " + rpcPort);

                            RunRemoteProcedureCall(rpcPort);
                        }

                        // Встречен неизвестный аргумент запуска
                        else
                            Console.WriteLine("Неизвестный аргумент запуска.\n" + MESSAGE_AVAILABLE_ARGS);

                        // Отключение от БД
                        connection.Close();
                    }
                    else
                        Console.WriteLine("Не удалось подключиться БД с заданными параметрами.");
                }
                else
                    Console.WriteLine("Не удалось загрузить параметры.");
            }
            // Иначе было встречено неверное количество аргументов запуска
            else
                Console.WriteLine("Неверное количество аргументов запуска.\n" + MESSAGE_AVAILABLE_ARGS);

            Console.WriteLine("Программа завершила работу.\nНажмите любую клавишу для выхода.");
            Console.ReadKey();
        }

        // Запускает прослушивание TCP-сокетом сервера
        private static void RunTCPSocket(in String importerPort)
        {
            Console.WriteLine("Запуск обмена данными с помощью TCP-сокетов.");

            TcpListener tcpListener = new TcpListener(IPAddress.Any, Convert.ToInt32(importerPort));
            tcpListener.Start();

            rsa = new RSACryptoServiceProvider(2048);
            rsaPublicKeySended = false;

            // TCP-клиент
            TcpClient tcpClient;

            // Сетевой поток клиента
            NetworkStream networkStream = null;

            Console.WriteLine("Ожидание подключения программы экспорта.");

            do
            {
                // Ожидание подключения TCP-клиента
                tcpClient = tcpListener.AcceptTcpClient();

            }
            while (!tcpClient.Connected);

            networkStream = tcpClient.GetStream();

            // Проверка, подключен ли TCP-сокет
            bool connected = true;

            // Данные для передачи/записи информации
            byte[]
                bytes = null,
                syncByte = SyncByte(false);

            while (connected)
            {
                // Если открытый ключ RSA 2048 ещё не был передан клиенту, то начинается процедура передачи
                if (!rsaPublicKeySended)
                {
                    Console.WriteLine("Передача открытого ключа RSA 2048.");

                    // Экспорт открытого ключа RSA 2048 для последующей передачи клиенту
                    byte[] xmlRsaPublicKey = Encoding.UTF8.GetBytes(rsa.ToXmlString(false));

                    // Передача клиенту размера сообщения с открытым ключом RSA 2048
                    bytes = BitConverter.GetBytes(xmlRsaPublicKey.Length);
                    networkStream.Write(bytes, 0, bytes.Length);

                    // Синхронизация с клиентом
                    networkStream.Read(syncByte, 0, syncByte.Length);

                    // Передача публичного ключа RSA 2048 клиенту
                    bytes = xmlRsaPublicKey;
                    networkStream.Write(bytes, 0, bytes.Length);

                    // Синхронизация с клиентом
                    networkStream.Read(syncByte, 0, syncByte.Length);

                    // Открытый ключ RSA 2048 был передан клиенту
                    rsaPublicKeySended = true;

                    Console.WriteLine("Полученная информация:");
                }

                // Ожидание получения информации от клиента
                while (tcpClient.Available == 0 && connected);

                // Данные от клиента
                if (connected)
                    bytes = new byte[tcpClient.Available];

                // Синхронизация с клиентом
                if (connected)
                {
                    syncByte = SyncByte(false);
                    networkStream.Write(syncByte, 0, syncByte.Length);
                }

                if (connected)
                {
                    // Чтение зашифрованных данных клиента
                    networkStream.Read(bytes, 0, bytes.Length);

                    // Дешифровка и обработка полученных данных 
                    String decrypt = DecryptData(bytes);
                    Console.WriteLine(decrypt);
                    SetPostgreSQLData(decrypt);

                    /*
                     * Получение от клиента статуса передачи сообщений
                     * Любое положительное число (от 1 до 255) - передача сообщений продолжается
                     * Ноль - передача сообщений завершается, клиент отключается
                     */
                    networkStream.Read(syncByte, 0, syncByte.Length);
                    connected = syncByte[0] > 0;
                }
            }

            if (networkStream != null)
                networkStream.Close();

            Console.WriteLine("Данные получены.\nОтключения программы экспорта.");
            tcpClient.Close();
            tcpListener.Stop();
        }

        // Запуск с помощью очереди сообщений
        private static void RunMessageQueue(
            in String messageQueueAddress, in String messageQueuePort,
            in String messageQueueUsername, in String messageQueuePassword,
            String messageQueueChannel)
        {
            Console.WriteLine("Запуск обмена данными с помощью очереди сообщений.");

            // Создание подключения
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = messageQueueAddress;
            factory.Port = Convert.ToInt32(messageQueuePort);
            factory.UserName = messageQueueUsername;
            factory.Password = messageQueuePassword;
            factory.VirtualHost = messageQueueChannel;

            // Осуществление подключения к очереди сообщений
            IConnection connection = null;

            try
            {
                connection = factory.CreateConnection();
            }
            catch (BrokerUnreachableException exception) 
            {
                connection = null;
            }

            // Если подключение к серверу установлено, то продолжается подключение к очереди сообщений
            if (connection != null)
            {
                // Подключение к каналу
                IModel channel = connection.CreateModel();
                channel.QueueDeclare(messageQueueChannel, false, false, false, null);
                channel.QueuePurge(messageQueueChannel);

                // Стандартные настройки
                IBasicProperties properties = channel.CreateBasicProperties();

                // Получение открытого и закрытого ключей RSA 2048
                rsa = new RSACryptoServiceProvider(2048);

                // Экспорт открытого ключа RSA 2048 для последующей передачи клиенту
                byte[] bytes = Encoding.UTF8.GetBytes(rsa.ToXmlString(false));

                Console.WriteLine("Передача открытого ключа RSA 2048");

                while (channel.BasicGet(messageQueueChannel, false) != null);

                // Отправка сообщения с открытым ключом RSA 2048
                channel.BasicPublish("", messageQueueChannel, properties, bytes);

                // Обработчик при получении сообщения с данными ненормализованной таблицы
                EventingBasicConsumer dataConsumer = new EventingBasicConsumer(channel);

                // Дешифрованное сообщение
                String decrypted = String.Empty;

                // Сообщение о списке полученных данных выведено
                bool dataPrintBegin = false;

                // Переопределение события получения сообщения
                dataConsumer.Received += (model, eventArgs) =>
                {
                    byte[] recieved = eventArgs.Body.ToArray();

                    // Отправка сообщения с открытым ключом RSA 2048, пока клиент его не получит
                    if (recieved.SequenceEqual(bytes))
                    {
                        channel.BasicPublish("", messageQueueChannel, properties, bytes);
                    }
                    // Иначе обработка полученного сообщения
                    else
                    {
                        if (!dataPrintBegin)
                        {
                            Console.WriteLine("Полученные данные:");
                            dataPrintBegin = true;
                        }

                        decrypted = DecryptData(recieved);

                        if (!decrypted.Equals(MESSAGE_SEPARATOR.ToString()))
                        {
                            Console.WriteLine(decrypted);
                            SetPostgreSQLData(decrypted);
                        }
                    }
                };

                // Получение сообщения с данными из ненормализованной таблицы
                channel.BasicConsume(messageQueueChannel, false, dataConsumer);

                while (!decrypted.Equals(MESSAGE_SEPARATOR.ToString()));

                Console.WriteLine("Данные получены.\nОтключения программы экспорта.");
            }
            else
                Console.WriteLine("Не удалось установить подключение по адресу: " + messageQueueAddress);
        }

        // Запуск с помощью вызова удаленных процедур
        private static void RunRemoteProcedureCall(in String rpcPort)
        {
            hostBuilder = CreateHostBuilder(new String[] {}, rpcPort).Build();

            Console.WriteLine("Запуск обмена данными с помощью вызова удаленных процедур.");
            Console.WriteLine("Полученные данные:");

            // Запуск сервиса
            hostBuilder.Run();

            Console.WriteLine("Данные получены.\nОтключения программы экспорта.");
        }

        // Принимает данные, переданные с  помощью вызова удаленных процедур
        public override Task<Reply> Send(Request request, ServerCallContext context)
        {
            // Полученное сообщение от программы экспорта
            String message = request.Data;

            // Если получено непустое сообщение, то осуществляется попытка вставить данные в базу данных
            if (!message.Equals(String.Empty))
            {
                Console.WriteLine(message);
                SetPostgreSQLData(message);
            }
            // Иначе передача сообщений завершена и происходит выключение сервиса
            else
               hostBuilder.StopAsync();

            // Ответ клиенту (пустая строка)
            Reply reply = new Reply();
            reply.Data = String.Empty;

            return Task.FromResult(reply);
        }

        // Добавление сервисов gRPC
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddGrpc(
                options =>
                {
                    // Отключение детализированных ошибок
                    options.EnableDetailedErrors = false;

                    // Отключение ответа на неизвестные службы
                    options.IgnoreUnknownServices = true;
                });
        }

        // Встраивает сервис gRPC в систему маршрутизации для обработки запроса
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                /**
                  * Встраивание сервиса в систему машрутизации.
                  * Т.к. данный класс является типом сервиса, поэтому он и передается.
                  */
                endpoints.MapGrpcService<Importer>(); // Т.к. наследуется от Greeter.Base

                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync("");
                });
            });
        }

        // Создает веб-подключение
        private static IHostBuilder CreateHostBuilder(string[] args, String rpcPort)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureLogging(
                logging =>
                {
                    // Отключение логирования по умолчанию (нет лишних сообщений от gRPC)
                    logging.AddFilter("Default", LogLevel.None);
                    logging.AddFilter("Microsoft", LogLevel.None);
                    logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.None);
                })
                .ConfigureWebHostDefaults(
                webBuilder =>
                {
                    // Прослушивание всех подключений по определенному порту и протоколу HTTPS
                    webBuilder.UseUrls(new String[] { "https://*:" + rpcPort });
                    webBuilder.UseStartup<Importer>();
                });
        }

        // Получает параметр из конфигурационного файла
        private static String GetConfigParam(in String path, in String param)
        {
            String result = null;

            // Если конфигурационный файл ещё не открыт, то осуществляется попытка его открыть
            if (config == null)
            {
                if (File.Exists(path))
                {
                    config = new XmlDocument();
                    config.Load(path);
                }
                else
                    Console.WriteLine(
                        "Указанного файла \"" + path + "\" не существует.\nИскомый файл: " +
                        CONFIG_PATH + "\n" +
                        MESSAGE_AVAILABLE_CONFIG);
            }

            // Если конфигурационный файл существует, то осуществляется попытка получить параметр
            if (config != null)
            {
                XmlNode xmlNode = config.SelectSingleNode(param);

                // Если параметр существует, то его значение запоминается
                if (xmlNode != null)
                    result = xmlNode.InnerText;
                else
                    Console.WriteLine("Не удалось найти параметр \"" + param + "\"\n" + MESSAGE_AVAILABLE_CONFIG);
            }

            return result;
        }


        // Записывает данные в БД PostgreSQL
        private static void SetPostgreSQLData(in String data)
        {
            // Список полученных данных
            List<String> list = new List<String>(data.Split(MESSAGE_SEPARATOR));

            // Столбцов должно быть строго 8
            if (list.Count == 8)
            {
                // Данные
                String
                    company_location = Quotes(list[0]),
                    company_name = Quotes(list[1]),
                    device_name = Quotes(list[2]),
                    device_price = Quotes(list[3]),
                    os_name = Quotes(list[4]),
                    os_vendor = Quotes(list[5]),
                    shop_name = Quotes(list[6]),
                    shop_phone = Quotes(list[7]);

                list.Clear();

                // Вставка информации в таблицы
                int
                    // Вставка в таблицу public.os
                    os_id = InsertPostgreSQLData(
                        "public.os",
                        new String[] { "name", "vendor" },
                        new String[] { os_name, os_vendor }),

                    // Вставка в таблицу public.companies
                    company_id = InsertPostgreSQLData(
                        "public.companies",
                        new String[] { "name", "location" },
                        new String[] { company_name, company_location }),

                    // Вставка в таблицу public.devices
                    device_id = InsertPostgreSQLData(
                        "public.devices",
                        new String[] { "name", "company_id", "os_id" },
                        new String[] { device_name, Convert.ToString(company_id), Convert.ToString(os_id) }),

                    // Вставка в таблицу public.shops
                    shop_id = InsertPostgreSQLData(
                        "public.shops",
                        new String[] { "name", "phone" },
                        new String[] { shop_name, shop_phone });

                // Вставка в таблицу public.prices
                InsertPostgreSQLData(
                    "public.prices",
                    new String[] { "shop_id", "device_id", "value" },
                    new String[] { Convert.ToString(shop_id), Convert.ToString(device_id), device_price });
            }
        }

        // Вставляет данные в таблицу, если они отсутствуют, и возвращает ID добавленной/найденной пары
        private static int InsertPostgreSQLData(in String tableName, in String[] columns, in String[] values)
        {
            // ID записи в таблице
            int id = -1;

            // Операция будет выполняться только если количество столбцов и значений совпадает
            if (columns.Length == values.Length)
            {
                int count = columns.Length;

                // Получение имен столбцов
                String columnsNames = null;

                for (int i = 0; i < count; i++)
                {
                    columnsNames += columns[i];

                    if (i < count - 1)
                        columnsNames += ", ";
                }

                if (String.IsNullOrWhiteSpace(columnsNames))
                    columnsNames = "*";

                // Генерация запроса на получение строки данных из таблицы
                command.CommandText = "SELECT " + columnsNames + " FROM " + tableName;

                // Условие проверки
                String statement = String.Empty;

                for (int i = 0; i < count; i++)
                {
                    if (i == 0)
                        statement += " WHERE ";

                    statement += columns[i] + " = " + values[i];

                    if (i < count - 1)
                        statement += " AND ";
                }

                command.CommandText += statement + ";";

                // Ответ от БД
                NpgsqlDataReader reader = command.ExecuteReader();

                // Запись полученных значений из запроса
                String value = null;

                // Пока доступны данные происходит их запись в полученные значения
                while (reader.Read())
                    for (int i = 0; i < reader.FieldCount; i++)
                        value += reader[i];

                // Очищаем строку запроса
                reader.Close();
                command.CommandText = String.Empty;

                // Если данных не было получено, значит, необходимо вставить полученные значения
                if (String.IsNullOrWhiteSpace(value))
                {
                    // Генерация запроса вставки
                    command.CommandText = "INSERT INTO " + tableName + " (";

                    for (int i = 0; i < count; i++)
                    {
                        command.CommandText += columns[i];

                        if (i < count - 1)
                            command.CommandText += ", ";
                    }

                    command.CommandText += ") VALUES (";

                    for (int i = 0; i < count; i++)
                    {
                        command.CommandText += values[i];

                        if (i < count - 1)
                            command.CommandText += ", ";
                    }

                    command.CommandText += ");";

                    // Выполнение запроса вставки
                    command.ExecuteNonQuery();
                }

                // Очищаем строку запроса
                command.CommandText = String.Empty;

                // Получение ID
                command.CommandText = "SELECT id FROM " + tableName + statement + ";";

                // Ответ от БД
                reader = command.ExecuteReader();

                // Пока доступны данные происходит их запись в полученные значения
                while (reader.Read())
                    id = Convert.ToInt32(reader[0]);

                // Очищаем строку запроса
                reader.Close();
                command.CommandText = String.Empty;
            }

            return id;
        }

        // Возвращает строку со вставленными в начало и конец одинарными кавычками
        private static String Quotes(in String text)
        {
            return "'" + text + "'";
        }

        // Возвращает дешифрованную строку, которая была зашифрована в AES 256 (ключ в свою очередь зашифрован в RSA 2048)
        private static String DecryptData(in byte[] data)
        {
            String result = "";

            // Размеры ключей шифрования
            int
                index = 0,
                encryptedAesSize = BitConverter.ToInt32(data, index),
                aesKeySize = BitConverter.ToInt32(data, index += 4),
                aesIVSize = BitConverter.ToInt32(data, index += 4);

            index += 4;

            // Считывание зашифрованных ключа и вектора инициализации AES 256
            byte[] encryptedAes = new byte[encryptedAesSize];

            for (int i = 0, dataCount = index + encryptedAesSize; i < encryptedAes.Length && index < dataCount; i++, index++)
                encryptedAes[i] = data[index];

            // Считывание дешифрованных ключа и вектора инициализации AES 256 
            byte[]
                decryptedAes = rsa.Decrypt(encryptedAes, false),
                aesKey = new byte[aesKeySize],
                aesIV = new byte[aesIVSize];

            for (int i = 0; i < decryptedAes.Length; i++)
                if (i < aesKey.Length)
                    aesKey[i] = decryptedAes[i];
                else
                    aesIV[i - aesKey.Length] = decryptedAes[i];

            // Дешифровка текста (симметричное шифрование AES 256)
            using (Aes aes = Aes.Create())
            {
                aes.Key = aesKey;
                aes.IV = aesIV;

                // Создание инструмента шифрования для трансформации массива байтов перед передачей в строку
                ICryptoTransform decryptor = aes.CreateDecryptor(aes.Key, aes.IV);

                // Зашифрованный текст
                byte[] encryptedText = new byte[data.Length - index];

                for (int i = index; i < data.Length; i++)
                    encryptedText[i - index] = data[i];

                /*
                 * Создание потоков, используемых для шифрования
                 * Поток, резервное хранилищем которого является память
                 */
                using (MemoryStream memoryStream = new MemoryStream(encryptedText))
                {
                    // Связывание потоков данных с криптографическим преобразованием
                    using (CryptoStream cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
                    {
                        // Запись дешифрованного текста в строку
                        using (StreamReader streamReader = new StreamReader(cryptoStream))
                        {
                            result = streamReader.ReadToEnd();
                        }
                    }
                }
            }

            return result;
        }

        // Генерирует массив из одного байта со случайным значением для синхронизации с сервером по TCP
        private static byte[] SyncByte(bool exitCode)
        {
            byte[] singleByte = new byte[1];
            Random random = new Random();
            singleByte[0] = exitCode ? (byte)random.Next(1, 255) : (byte)0;
            return singleByte;
        }
    }
}

