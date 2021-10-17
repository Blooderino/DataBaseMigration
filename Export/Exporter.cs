using System;
using System.Net.Sockets;
using System.Xml;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using System.Data.SQLite;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

using Grpc.Net.Client;
using Grpc.Core;

namespace DataBaseMigration
{
    class Exporter
    {
        // Аргумент запуска с помощью веб-сокетов
        private const String RUN_TCP_SOCKET = "--run-tcp-socket";

        // Аргумент запуска с помощью очереди сообщений
        private const String RUN_MESSAGE_QUEUE = "--run-message-queue";

        // Аргумент запуска с помощью вызова удаленных процедур
        private const String RUN_REMOTE_PROCEDURE_CALL = "--run-remote-procedure-call";

        // Публичный ключ RSA 2048
        private static RSACryptoServiceProvider rsa;

        // Текстовое сообщение с доступными аргументами запуска
        private const String MESSAGE_AVAILABLE_ARGS =
            "Доступные аргументы запуска:\n" +
            "1. Передача данных через сокеты: " + RUN_TCP_SOCKET + "\n" +
            "2. Передача данных через очередь сообщений: " + RUN_MESSAGE_QUEUE + "\n" +
            "3. Передача данных через вызов удаленных процедур: " + RUN_REMOTE_PROCEDURE_CALL + "\n" +
            "Одновременно можно использовать только один аргумент запуска.";

        // Путь до конфигурационного файла
        private const String CONFIG_PATH = "config.xml";

        // Параметр адреса сервера сокетов из конфигурационного файла
        private const String CONFIG_SOCKET_ADDRESS = "/export/socket/address";

        // Параметр порта сервера сокетов из конфигурационного файла
        private const String CONFIG_SOCKET_PORT = "/export/socket/port";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_ADDRESS = "/export/message-queue/address";

        // Параметр порта сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_PORT = "/export/message-queue/port";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_USERNAME = "/export/message-queue/username";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_PASSWORD = "/export/message-queue/password";

        // Параметр имени очереди сообщений из конфигурационного файла
        private const String CONFIG_MESSAGE_QUEUE_CHANNEL = "/export/message-queue/channel";

        // Параметр адреса сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_REMOTE_PROCEDURE_CALL_ADDRESS = "/export/remote-procedure-call/address";

        // Параметр порта сервера очереди сообщений из конфигурационного файла
        private const String CONFIG_REMOTE_PROCEDURE_CALL_PORT = "/export/remote-procedure-call/port";

        // Параметр базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_PATH = "/export/database/path";

        // Параметр таблицы базы данных из конфигурационного файла
        private const String CONFIG_DATABASE_TABLE = "/export/database/table";

        // Текстовое сообщение с доступным форматом файла
        private const String MESSAGE_AVAILABLE_CONFIG =
            "Доступный формат конфигурационного файла:\n" +
            "<export>\n" +
            "   <socket>\n" +
            "       <address>IP или домен сервера сокетов</address>\n" +
            "       <port>Порт сервера сокетов</port>\n" +
            "   </socket>\n" +
            "   <message-queue>\n" +
            "       <address>IP или домен сервера очереди сообщений</address>\n" +
            "       <port>Порт сервера очереди сообщений</port>\n" +
            "       <username>Имя пользователя очереди сообщений</username>\n" +
            "       <password>Пароль пользователя очереди сообщений</password>\n" +
            "       <channel>Имя очереди сообщений</channel>" +
            "   </message-queue>\n" +
            "   <remote-procedure-call>\n" +
            "       <address>IP или домен сервера удаленного вызова процедур</address>\n" +
            "       <port>Порт сервера удаленного вызова процедур</port>\n" +
            "   </remote-procedure-call>\n" +
            "   <database>\n" +
            "       <path>Путь к файлу БД</path>\n" +
            "       <table>Имя таблицы БД</table>\n" +
            "   </database>\n" +
            "</export>";

        // Конфигурационный файл в формате XML-документа
        private static XmlDocument config = null;

        // Главный метод программы
        static async Task Main(string[] args)
        {
            Console.WriteLine("Запуск процесса экспорта данных.");

            // Если был встречен ровно 1 аргумент запуска, то начинается чтение параметров из конфигурационного файла
            if (args.Length == 1)
            {
                // Получение параметров из конфигурационного файла
                String
                    socketAddress = GetConfigParam(CONFIG_PATH, CONFIG_SOCKET_ADDRESS),
                    socketPort = GetConfigParam(CONFIG_PATH, CONFIG_SOCKET_PORT),
                    messageQueueAddress = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_ADDRESS),
                    messageQueuePort = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_PORT),
                    messageQueueUsername = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_USERNAME),
                    messageQueuePassword = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_PASSWORD),
                    messageQueueChannel = GetConfigParam(CONFIG_PATH, CONFIG_MESSAGE_QUEUE_CHANNEL),
                    pathToDataBase = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_PATH),
                    tableName = GetConfigParam(CONFIG_PATH, CONFIG_DATABASE_TABLE),
                    rpcAddress = GetConfigParam(CONFIG_PATH, CONFIG_REMOTE_PROCEDURE_CALL_ADDRESS),
                    rpcPort = GetConfigParam(CONFIG_PATH, CONFIG_REMOTE_PROCEDURE_CALL_PORT);

                // Если параметры не пустые, то переходим к выбору подключения к серверу
                if (socketAddress != null && socketPort != null && 
                    messageQueueAddress != null && messageQueuePort != null && 
                    messageQueueUsername != null && messageQueuePassword != null &&
                    messageQueueChannel != null &&
                    pathToDataBase != null && tableName != null &&
                    rpcAddress != null && rpcPort != null)
                {
                    Console.WriteLine(
                    "Параметры подключения:\n" +
                    "Путь к БД: " + pathToDataBase + "\n" +
                    "Имя таблицы: " + tableName);

                    // Запуск передачи данных через TCP-сокет
                    if (args[0].Equals(RUN_TCP_SOCKET))
                    {
                        Console.WriteLine("Адрес сервера сокетов: " + socketAddress + ":" + socketPort);

                        RunTCPSocket(socketAddress, socketPort, pathToDataBase, tableName);
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
                            messageQueueChannel,
                            pathToDataBase, tableName);
                    }

                    // Запуск передачи данных через вызов удаленных процедур
                    else if (args[0].Equals(RUN_REMOTE_PROCEDURE_CALL))
                    {
                        Console.WriteLine("Адрес сервера удаленного вызова процедур: " + rpcAddress + ":" + rpcPort);

                        await RunRemoteProcedureCall(rpcAddress, rpcPort, pathToDataBase, tableName);
                    }

                    // Встречен неизвестный аргумент запуска
                    else
                        Console.WriteLine("Неизвестный аргумент запуска.\n" + MESSAGE_AVAILABLE_ARGS);
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

        // Запуск с помощью веб-сокетов
        private static void RunTCPSocket(in String socketAddress, in String socketPort, in String pathToDataBase, in String tableName)
        {
            Console.WriteLine("Подключение с помощью TCP-сокета.");

            // Создание нового TCP-клиента
            TcpClient tcpClient = new TcpClient();

            // Подключение к серверу (осущетсвляет 10 попыток подключения)
            for (byte i = 0; i < 10 && !tcpClient.Connected; i++)
            {
                try
                {
                    tcpClient.Connect(socketAddress, Convert.ToInt32(socketPort));
                }
                catch (SocketException exception) {}
            }

            // Проверка, подключен ли клиент к серверу
            if (tcpClient.Connected)
            {
                // Сетевой поток клиента
                NetworkStream networkStream = tcpClient.GetStream();

                Console.WriteLine("Подключение установлено.\nПолучение открытого ключа RSA 2048.");

                // Получение открытого ключа RSA 2048
                rsa = new RSACryptoServiceProvider(2048);

                // Получение размера сообщения с открытым ключом RSA 2048
                byte[] bytes = new byte[4];

                // Синхронизация с сервером
                byte[] syncByte = SyncByte(false);
                networkStream.Write(syncByte, 0, syncByte.Length);

                // Чтение байтов из сетевого потока
                networkStream.Read(bytes, 0, bytes.Length);

                // Размер сообщения с открытым ключом RSA 2048
                int bytesSize = BitConverter.ToInt32(bytes, 0);

                // Набор байтов из полученного сообщения
                bytes = new byte[bytesSize];

                // Синхронизация с сервером
                syncByte = SyncByte(false);
                networkStream.Write(syncByte, 0, syncByte.Length);

                // Получение открытого ключа RSA 2048
                networkStream.Read(bytes, 0, bytes.Length);

                // Сохранение ключа RSA 2048
                rsa.FromXmlString(Encoding.UTF8.GetString(bytes));
                Console.WriteLine("Получение данных из ненормализованной БД.");

                // Получение данных из ненормализованной БД
                List<String> data = GetSQLiteData(pathToDataBase, tableName);

                Console.WriteLine("Отправка данных в программу импорта.");
                Console.WriteLine("Данные в ненормализованной таблице:");

                // Отправка данных серверу (построчно)
                for (int i = 0; i < data.Count; i++)
                {
                    // Отправка данных на сервер
                    Console.WriteLine(data[i]);
                    bytes = EncryptData(data[i]);
                    networkStream.Write(bytes, 0, bytes.Length);

                    // Синхронизация с сервером
                    networkStream.Read(syncByte, 0, syncByte.Length);

                    /*
                     * Передача серверу статуса передачи сообщений
                     * Любое положительное число (от 1 до 255) - передача сообщений продолжается
                     * Ноль - передача сообщений завершается, клиент отключается
                     */ 
                    syncByte = SyncByte(i == data.Count - 1);
                    networkStream.Write(syncByte, 0, syncByte.Length);
                }

                Console.WriteLine("Данные отправлены.\nОтключение от программы импорта.");
                
                // Закрытие сетевого потока
                networkStream.Close();
            }
            else
                Console.WriteLine("Не удалось подключиться к программе импорта по адресу: " + socketAddress + ":" + socketPort);

            // Закрытие TCP-клиента
            tcpClient.Close();
        }

        // Запуск с помощью очереди сообщений
        private static void RunMessageQueue(
            in String messageQueueAddress, in String messageQueuePort, 
            in String messageQueueUsername, in String messageQueuePassword, 
            String messageQueueChannel,
            in String pathToDataBase, in String tableName)
        {
            Console.WriteLine("Подключение с помощью очереди сообщений.");

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
                // Получение данных из ненормализованной БД
                List<String> data = GetSQLiteData(pathToDataBase, tableName);

                // Добавляем последнее сообщение, которое будет означать конец передачи данных
                data.Add(MESSAGE_SEPARATOR);

                // Подключение к каналу
                IModel channel = connection.CreateModel();
                channel.QueueDeclare(messageQueueChannel, false, false, false, null);

                // Получение открытого ключа RSA 2048
                rsa = new RSACryptoServiceProvider(2048);

                // Обработчик при получении сообщения с открытым ключом RSA 2048
                EventingBasicConsumer rsaConsumer = new EventingBasicConsumer(channel);

                // Стандартные настройки
                IBasicProperties properties = channel.CreateBasicProperties();

                // Полученный ключ шифрования
                String key = null;

                // Переопределение события получения сообщения
                rsaConsumer.Received += (model, eventArgs) =>
                {
                    key = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
                    rsa.FromXmlString(key);
                };

                // Получение сообщения с ключом
                while (String.IsNullOrWhiteSpace(key))
                    channel.BasicConsume(messageQueueChannel, false, rsaConsumer);

                channel.QueuePurge(messageQueueChannel);

                Console.WriteLine("Отправка данных в программу импорта.");
                Console.WriteLine("Данные в ненормализованной таблице:");

                // Отправка данных серверу (построчно)
                for (int i = 0; i < data.Count; i++)
                {
                    if (!data[i].Equals(MESSAGE_SEPARATOR))
                        Console.WriteLine(data[i]);

                    byte[] bytes = EncryptData(data[i]);
                    channel.BasicPublish("", messageQueueChannel, properties, bytes);
                }

                Console.WriteLine("Данные отправлены.\nОтключение от программы импорта.");
                channel.Close();
                connection.Close();
            }
            else
                Console.WriteLine("Не удалось установить подключение по адресу: " + messageQueueAddress);
        }

        // Запуск с помощью вызова удаленных процедур
        private static async Task RunRemoteProcedureCall(String address, String port, String pathToDataBase, String tableName)
        {
            // Все равно использовать недоверенные сертификаты
            HttpClientHandler httpHandler = new HttpClientHandler();
            httpHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;

            // Cоздание канала для обмена сообщениями с программой импорта
            GrpcChannel channel = GrpcChannel.ForAddress(
                "https://" + address + ":" + port,
                new GrpcChannelOptions { HttpHandler = httpHandler });

            // Создание клиента
            Greeter.GreeterClient client = new Greeter.GreeterClient(channel);

            // Получение данных из ненормализованной БД
            List<String> data = GetSQLiteData(pathToDataBase, tableName);

            for (int i = 0; i < data.Count; i++)
            {
                Console.WriteLine(data[i]);
                await client.SendAsync(new Request { Data = data[i] });
            }

            // Отправляет серверу сообщение о завершении передачи данных
            await client.SendAsync(new Request { Data = String.Empty });

            // Отключение канала для обмена сообщениями с программой импорта
            await channel.ShutdownAsync();
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

        // Раздилитель строк в сообщении
        private const String MESSAGE_SEPARATOR = "\t";

        // Подключается к БД SQLite и получает данные
        private static List<String> GetSQLiteData(in String pathToDataBase, in String tableName)
        {
            // Список всех строк ненормализованной БД
            List<String> result = new List<String>();

            // Запрос на получение данных из БД
            String query = "SELECT * FROM " + tableName;

            // Подключение к БД
            SQLiteConnection connection = new SQLiteConnection("Data Source=" + pathToDataBase + "; Version=3;");

            // Процесс подключения к БД
            connection.Open();

            // Команда для выполния у подключенной БД
            SQLiteCommand command = new SQLiteCommand(query, connection);

            // Считывание данных из БД
            SQLiteDataReader reader = command.ExecuteReader();

            try
            {
                while (reader.Read())
                {
                    // Строка, полученная из БД
                    String row = "";

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row += reader.GetString(i);

                        if (i < reader.FieldCount - 1)
                            row += MESSAGE_SEPARATOR;
                    }

                    result.Add(row);
                }
            }
            finally
            {
                // Отключение от БД
                reader.Close();
                connection.Close();
            }

            return result;
        }

        // Возвращает массив байтов, содержащий зашифрованный текст, зашифрованный ключ AES 256 и открытую часть ключа RSA 2048
        private static byte[] EncryptData(in String text)
        {
            byte[]
                result = null,
                textEncrypted = null,
                aesKey = null,
                aesIV = null;

            // Симмитричное шифрование AES 256 (шифрование исходного текста)
            using (AesCryptoServiceProvider aes = new AesCryptoServiceProvider())
            {
                // Устанавливаем размер ключа
                aes.KeySize = 256;

                // Генерация ключа и его вектора инициализации
                aes.GenerateIV();
                aes.GenerateKey();

                // Сохранение ключа шифрования и его вектора инициализации
                aesKey = aes.Key;
                aesIV = aes.IV;

                // Создание инструмента шифрования для трансформации текста перед передачей в массив байтов
                ICryptoTransform encryptor = aes.CreateEncryptor(aesKey, aesIV);

                /*
                 * Создание потоков, используемых для шифрования
                 * Поток, резервное хранилищем которого является память
                 */
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    // Связывание потоков данных с криптографическим преобразованием
                    using (CryptoStream cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                    {
                        // Запись данных в поток для криптографического преобразования
                        using (StreamWriter streamWriter = new StreamWriter(cryptoStream))
                        {
                            streamWriter.Write(text);
                        }

                        // Зашифрованный текст
                        textEncrypted = memoryStream.ToArray();
                    }
                }
            }

            // Шифрование ключа AES 256
            byte[] aesFull = new byte[aesKey.Length + aesIV.Length];
            aesKey.CopyTo(aesFull, 0);
            aesIV.CopyTo(aesFull, aesKey.Length);

            byte[] aesEncrypted = rsa.Encrypt(aesFull, false);

            // Размеры ключей RSA 2048 и AES 256
            byte[]
                aesEncryptedSize = BitConverter.GetBytes(aesEncrypted.Length),
                aesKeySize = BitConverter.GetBytes(aesKey.Length),
                aesIVSize = BitConverter.GetBytes(aesIV.Length),
                keysSizes = new byte[aesEncryptedSize.Length + aesKeySize.Length + aesIVSize.Length];

            aesEncryptedSize.CopyTo(keysSizes, 0);
            aesKeySize.CopyTo(keysSizes, aesEncryptedSize.Length);
            aesIVSize.CopyTo(keysSizes, aesEncryptedSize.Length + aesKeySize.Length);

            // Объединение всех данных в единный массив
            result = new byte[keysSizes.Length + aesEncrypted.Length + textEncrypted.Length];
            keysSizes.CopyTo(result, 0);
            aesEncrypted.CopyTo(result, keysSizes.Length);
            textEncrypted.CopyTo(result, keysSizes.Length + aesEncrypted.Length);

            return result;
        }

        // Генерирует массив из одного байта со случайным значением для синхронизации с сервером по TCP
        private static byte[] SyncByte(bool exit)
        {
            byte[] singleByte = new byte[1];
            Random random = new Random();
            singleByte[0] = exit ? (byte) 0 : (byte) random.Next(1, 255);
            return singleByte;
        }
    }
}
