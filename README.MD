# DataBaseMigration
Educational project (PSU, ITS-2018), migration from not normalized database (SQLite) to normalized database (PostgreSQL) by TCP-sockets, message queue (RabbitMQ) or remote procedure calls (gRPC).

## Pacakages configuration:
You need to restore pacakages. Learn more: https://docs.microsoft.com/en-us/nuget/consume-packages/package-restore#restore-using-the-nugetexe-cli

## Projects configuration:
You need rebuild every project for generation object-files for gRPC.

## Import database structure
Name: dbmigration_import

Schema: public

Tables:
1. companies (id, name, location);
2. devices (id, name, company_id, os_id);
3. os (id, name, vendor);
4. prices (id, shop_id, device_id, value);
5. shops (id, name, phone).

## Export database structure
See dbmigration_export.db in Export/Boot

## Network configuration:
1. All IP/domains must be reachable for clients.
2. All ports must be open for clients.

## PostgreSQL configuration:
1. Recomended version: 12.0 or higher
2. Used port: 5432 (by default)
3. User (login/password, by default): postgres postgres
4. Database name: dbmigration_import 
5. Database schema: public

## RabbitMQ configuration:
1. Recomended version: 3.8 or higher
2. Create new user from CLI: rabbitmqctl add_user dbmigrator dbmigrator
3. Create new channel: rabbitmqctl add_vhost database-migration
4. Give that user read/write permissions: rabbitmqctl set_permissions -p database-migration dbmigrator ".*" ".*" ".*"

## gRPC TLS trust configuration:
In project untrusted/invalid certificates are available. For educational proposes only!
