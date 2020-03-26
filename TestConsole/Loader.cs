using System;
using System.Reflection;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data;
using DataReaderProjector;
using DataReaderProjector.Dynamic;

namespace TestConsole
{
    public static class Loader
    {
        readonly static ConcurrentDictionary<string, object> cacheDeserializators = new ConcurrentDictionary<string, object>();

        static DeserializerDelegate<T> GetCacheDeserializator<T>(this DbDataReader reader, string key, string[,] commandToObjectMap)
            where T : class
        {
            return cacheDeserializators
                .GetOrAdd(key, (_) => reader.CreateDeserializer<T>(commandToObjectMap)) as DeserializerDelegate<T>;
        }

        static DeserializerDelegate<T> GetCacheDeserializator<T>(this DbDataReader reader, string key, Map<T> map)
            where T : class
        {
            return cacheDeserializators
                .GetOrAdd(key, (_) => reader.CreateClassDeserializer(map)) as DeserializerDelegate<T>;
        }


        static DeserializerDelegate<T> CreateDeserializer<T>(this DbDataReader reader, string[,] commandToObjectMap)
            where T : class
        {
            // dynamic is passed in as Object ... by c# design
            if (typeof(T) == typeof(object) /*|| typeof(T) == typeof(DynObj)*/)
            {
                return reader.CreateDynamicDeserializer<T>(commandToObjectMap);
            }
            else
            {
                if (typeof(T).IsClass && typeof(T) != typeof(string))
                {
                    return reader.CreateClassDeserializer<T>(commandToObjectMap);
                }
                else
                    throw new NotSupportedException();
            }
        }

        public static IEnumerable<T> Enumerate<T>(this DbConnection connection,
            CommandType commandType, string commandText, DbParameter[] commandParameters, string[,] commandToObjectMap) where T : class
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (commandText == null)
                throw new ArgumentNullException(nameof(commandText));
            if (commandToObjectMap == null)
                throw new ArgumentNullException(nameof(commandToObjectMap));

            using var cmd = connection.CreateCommand();
            cmd.CommandType = commandType;
            cmd.CommandText = commandText;
            if (commandParameters != null)
            {
                foreach (var p in commandParameters)
                    cmd.Parameters.Add(p);
            }

            using var reader = cmd.ExecuteReader();

            if (reader.Read())
            {
                var key = commandText.GetHashCode() + "@" + connection.DataSource.GetHashCode() + "@" + typeof(T).FullName.GetHashCode();
                var deserializator = reader.GetCacheDeserializator<T>(key, commandToObjectMap);
                var more = false;
                do
                {
                    yield return deserializator(reader, out more);
                } while (more);
            }
        }

        public static TimeSpan Execution { get; private set; }

        public static TimeSpan Build { get; private set; }

        public static TimeSpan Projection { get; private set; }

        public static IEnumerable<T> Enumerate<T>(this DbConnection connection,
            CommandType commandType, string commandText, DbParameter[] commandParameters, Map<T> map)
            where T : class
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (commandText == null)
                throw new ArgumentNullException(nameof(commandText));
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            using DbCommand cmd = connection.CreateCommand();
            cmd.CommandType = commandType;
            cmd.CommandText = commandText;
            if (commandParameters != null)
            {
                foreach (var p in commandParameters)
                    cmd.Parameters.Add(p);
            }

            var begin = DateTime.Now;
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                Execution = DateTime.Now.Subtract(begin);
                begin = DateTime.Now;

                var key = commandText.GetHashCode() + "@" + connection.DataSource.GetHashCode() + "@" + map.GetHashCode();
                var deserializator = reader.GetCacheDeserializator(key, map);

                Build = DateTime.Now.Subtract(begin);
                begin = DateTime.Now;

                var more = false;
                do
                {
                    yield return deserializator(reader, out more);
                } while (more);

                Projection = DateTime.Now.Subtract(begin);
            }
        }

        public static List<T> Load<T>(this DbConnection connection,
            CommandType commandType, string commandText, DbParameter[] commandParameters, string[,] commandToObjectMap)
            where T : class
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (commandText == null)
                throw new ArgumentNullException(nameof(commandText));
            if (commandToObjectMap == null)
                throw new ArgumentNullException(nameof(commandToObjectMap));

            using DbCommand cmd = connection.CreateCommand();
            cmd.CommandType = commandType;
            cmd.CommandText = commandText;
            if (commandParameters != null)
            {
                foreach (DbParameter p in commandParameters)
                    cmd.Parameters.Add(p);
            }

            using DbDataReader reader = cmd.ExecuteReader();

            var container = new List<T>(10);
            if (reader.Read())
            {
                var key = commandText.GetHashCode() + "@" + connection.DataSource.GetHashCode() + "@" + typeof(T).FullName.GetHashCode();
                var deserializator = reader.GetCacheDeserializator<T>(key, commandToObjectMap);
                var more = false;
                do
                {
                    container.Add(deserializator(reader, out more));
                } while (more);
            }
            return container;
        }
    }
}