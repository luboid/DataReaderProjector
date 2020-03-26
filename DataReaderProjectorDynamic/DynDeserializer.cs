using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace DataReaderProjector.Dynamic
{
    public static class DynDeserializer
    {
        public static DeserializerDelegate<T> CreateDynamicDeserializer<T>(this DbDataReader reader, string[,] commandToObjectMap)
            where T : class
        {
            DynDeserializerItem doi = ParseMap(reader, commandToObjectMap);

            return new DeserializerDelegate<T>(
                (new DynDeserializerUtil(doi)).Deserializer<T>);
        }

        class DynDeserializerUtil
        {
            readonly DynDeserializerItem _dynObjItem;

            public DynDeserializerUtil(DynDeserializerItem dynObjItem)
            {
                _dynObjItem = dynObjItem;
            }

            public T Deserializer<T>(DbDataReader reader, out bool more) where T : class
            {
                more = false;
                Dictionary<int, object> _pkContext = new Dictionary<int, object>();

                object index_value_1 = reader.GetValue(_dynObjItem.Index);
                object index_value_2;

                Dictionary<string, object> values = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
                do
                {
                    InternalDeserializer(values, _pkContext, reader, _dynObjItem);

                    if (reader.Read())
                    {
                        index_value_2 = reader.GetValue(_dynObjItem.Index);
                        if ((index_value_2 as IComparable).CompareTo(index_value_1) != 0)
                        {
                            more = true;
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }

                } while (true);

                return DynObj.Create(values) as T;
            }

            void InternalDeserializer(Dictionary<string, object> upContext, Dictionary<int, object> pkContext, DbDataReader reader, DynDeserializerItem o)
            {
                Dictionary<string, object> values;
                foreach (KeyValuePair<string, int> kv in o.Fields)
                    upContext[kv.Key] = reader.GetValue(kv.Value);

                List<object> list = new List<object>(); object a;
                foreach (KeyValuePair<string, DynDeserializerItem> kv in o.Collections)
                {
                    if (kv.Value.Index != -1)
                    {
                        if (reader.IsDBNull(kv.Value.Index))
                        {
                            if (!upContext.ContainsKey(kv.Key))
                            {
                                upContext.Add(kv.Key, (dynamic)null);
                            }
                            continue;
                        }

                        object pk = reader.GetValue(kv.Value.Index);
                        if (pkContext.ContainsKey(kv.Value.Index))
                        {
                            if ((pkContext[kv.Value.Index] as IComparable).CompareTo(pk) > 0)
                            {
                                if (!upContext.ContainsKey(kv.Key))
                                {
                                    upContext.Add(kv.Key, (dynamic)null);
                                }
                                continue;
                            }
                        }

                        pkContext[kv.Value.Index] = pk;
                    }

                    values = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);
                    if (kv.Value.Single)
                    {
                        upContext.Add(kv.Key, (dynamic)DynObj.Create(values));
                    }
                    else
                    {
                        upContext.TryGetValue(kv.Key, out a);
                        if (null == (list = a as List<object>))
                        {
                            list = new List<object>();
                            upContext[kv.Key] = list;
                        }

                        list.Add((dynamic)DynObj.Create(values));
                    }

                    InternalDeserializer(values, pkContext, reader, kv.Value);
                }
            }

        }

        static DynDeserializerItem ParseMap(DbDataReader reader, string[,] commandToObjectMap)
        {
            if (commandToObjectMap == null)
                return null;

            string name;
            string[] fieldNameItems;
            char[] fieldSplit = new char[] { '.' };

            Dictionary<string, DynDeserializerItem> searchItems;
            DynDeserializerItem items = new DynDeserializerItem();
            DynDeserializerItem item = null; bool single = false;

            for (int i = 0, l = commandToObjectMap.Length / 2; i < l; i++)
            {
                fieldNameItems = commandToObjectMap[i, 0].Split(fieldSplit);
                if (1 == fieldNameItems.Length)
                {
                    name = fieldNameItems[0];
                    try
                    {
                        if ("PrimaryKey" == name)
                            items.Index = reader.GetOrdinal(commandToObjectMap[i, 1]);
                        else
                            items.Fields[name] = reader.GetOrdinal(commandToObjectMap[i, 1]);
                    }
                    catch (IndexOutOfRangeException)
                    {
                        continue;
                    }
                }
                else
                {
                    searchItems = items.Collections;
                    for (int j = 0, jl = fieldNameItems.Length - 1; j <= jl; j++)
                    {
                        name = fieldNameItems[j];
                        single = name.EndsWith(":1");
                        if (single)
                            name = name.Substring(0, name.Length - 2);

                        if (searchItems.TryGetValue(name, out item))
                        {
                            searchItems = item.Collections;
                        }
                        else
                        {
                            item = new DynDeserializerItem() { Single = single };
                            searchItems[name] = item;
                            searchItems = item.Collections;
                        }

                        if ((j + 1) == jl)
                        {
                            try
                            {
                                name = fieldNameItems[j + 1];
                                if ("PrimaryKey" == name)
                                {
                                    item.Index = reader.GetOrdinal(commandToObjectMap[i, 1]);
                                }
                                else
                                {
                                    item.Fields[name] = reader.GetOrdinal(commandToObjectMap[i, 1]);
                                }
                            }
                            catch (IndexOutOfRangeException)
                            { }

                            break;
                        }
                    }
                }
            }

            try
            {
                if (-1 == items.Index)
                    items.Index = reader.GetOrdinal("Id");
            }
            catch (IndexOutOfRangeException)
            { }

            return items;
        }
    }
}