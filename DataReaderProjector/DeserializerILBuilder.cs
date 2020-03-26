using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection.Emit;
using System.Data.Common;
using System.Reflection;

namespace DataReaderProjector
{
    public static class DeserializerILBuilder
    {
        static readonly MethodInfo IComparable_CompareTo = typeof(System.IComparable).GetMethod("CompareTo", new Type[] { typeof(object) });
        static readonly MethodInfo DbDataReader_Read = typeof(DbDataReader).GetMethod("Read", Type.EmptyTypes);
        static readonly MethodInfo DbDataReader_GetValue = typeof(DbDataReader).GetMethod("GetValue", new Type[] { typeof(int) });
        static readonly MethodInfo DbDataReader_IsDBNull = typeof(DbDataReader).GetMethod("IsDBNull", new Type[] { typeof(int) });
        static readonly MethodInfo Convert_ChangeType = typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(object), typeof(Type) });
        static readonly MethodInfo Type_GetTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle", new Type[] { typeof(RuntimeTypeHandle) });

        public static DeserializerDelegate<T> CreateClassDeserializer<T>(this DbDataReader reader, string[,] commandToObjectMap)
            where T : class
        {
            ushort vars = 0;
            Type t = typeof(T);

            var dm = new DynamicMethod(string.Format("Deserialize_{0}", Guid.NewGuid().ToString("N")), typeof(T), new[] {
                typeof(DbDataReader), typeof(bool).MakeByRefType() }, true);

            ILGenerator il = dm.GetILGenerator();

            // initialize function argument 1
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stind_I1);

            // create object who function deserialize
            il.Emit(OpCodes.Newobj, t.GetConstructor(Type.EmptyTypes)); // stack is now [target]

            GenerateILCode(ref vars, 0, string.Empty, commandToObjectMap, il, reader, t);

            // return desirialized object
            il.Emit(OpCodes.Ret);


            return (DeserializerDelegate<T>)dm.CreateDelegate(typeof(DeserializerDelegate<T>));
        }

        public static DeserializerDelegate<T> CreateClassDeserializer<T>(this DbDataReader reader, Map<T> map)
            where T : class
        {
            ushort vars = 0;

            var dm = new DynamicMethod(string.Format("Deserialize_{0}", Guid.NewGuid().ToString("N")), typeof(T), new[] {
                typeof(DbDataReader), typeof(bool).MakeByRefType() }, true);

            ILGenerator il = dm.GetILGenerator();

            // initialize function argument 1
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stind_I1);

            // create object who function deserialize
            il.Emit(OpCodes.Newobj, map.Type.GetConstructor(Type.EmptyTypes)); // stack is now [target]

            GenerateILCode(ref vars, 0, map, il, reader);

            // return desirialized object
            il.Emit(OpCodes.Ret);


            return (DeserializerDelegate<T>)dm.CreateDelegate(typeof(DeserializerDelegate<T>));
        }

        static void GenerateILCode(ref ushort vars, int level, string namePrefix, string[,] commandToObjectMap, ILGenerator il, DbDataReader reader, Type type)
        {
            ushort primary_key_value_idx = 0;
            ushort primary_key_value_second_idx = 0;

            Label nomore = new Label(); Label loopCollection = nomore;
            List<PropertyInfo> properties = null;
            List<FieldInfo> fields = null;

            foreach (PropertyInfo p in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                if (GenerateILClass(ref vars, level + 1, namePrefix, commandToObjectMap, il, reader, p.Name, p.PropertyType, p.GetSetMethod()))
                {
                    if (null == properties)
                        properties = new List<PropertyInfo>();

                    properties.Add(p);
                }

            foreach (FieldInfo f in type.GetFields(BindingFlags.Instance | BindingFlags.Public))
                if (GenerateILClass(ref vars, level + 1, namePrefix, commandToObjectMap, il, reader, f.Name, f.FieldType, f))
                {
                    if (null == fields)
                        fields = new List<FieldInfo>();

                    fields.Add(f);
                }

            int primary_key_index = GetPrimaryKey(namePrefix, commandToObjectMap, reader);// get primary key index
            if ((null != properties || null != fields) && (0 != level || -1 != primary_key_index))
            {
                if (0 == level)
                {
                    // cycle by records break on primary key change
                    nomore = il.DefineLabel();
                    loopCollection = il.DefineLabel();

                    il.DeclareLocal(typeof(object));
                    primary_key_value_idx = vars; ++vars;

                    il.DeclareLocal(typeof(object));
                    primary_key_value_second_idx = vars; ++vars;

                    // get primary key value
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, primary_key_index);
                    il.Emit(OpCodes.Callvirt, DbDataReader_GetValue);
                    il.Emit(OpCodes.Stloc, primary_key_value_second_idx);

                    il.MarkLabel(loopCollection);

                    il.Emit(OpCodes.Ldloc, primary_key_value_second_idx);
                    il.Emit(OpCodes.Stloc, primary_key_value_idx);
                }

                if (null != properties)
                    foreach (PropertyInfo p in properties)
                        GenerateILCollection(ref vars, level + 1, namePrefix + p.Name + ".", commandToObjectMap, il, reader, p.PropertyType, p.GetGetMethod(), p.GetSetMethod());

                if (null != fields)
                    foreach (FieldInfo f in fields)
                        GenerateILCollection(ref vars, level + 1, namePrefix + f.Name + ".", commandToObjectMap, il, reader, f.FieldType, f, f);

                if (0 == level)
                {
                    // cycle by records break on primary key change
                    // read next record
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Callvirt, DbDataReader_Read);

                    // if no more records goto to nomore label
                    il.Emit(OpCodes.Brfalse_S, nomore);

                    // get next record primary key value
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, primary_key_index);
                    il.Emit(OpCodes.Callvirt, DbDataReader_GetValue);

                    // store value for next loop
                    il.Emit(OpCodes.Stloc, primary_key_value_second_idx);
                    il.Emit(OpCodes.Ldloc, primary_key_value_second_idx);

                    // save into IComparable variable with cast???
                    // compare prev and next record primary key
                    il.Emit(OpCodes.Isinst, typeof(System.IComparable));
                    il.Emit(OpCodes.Ldloc, primary_key_value_idx);
                    il.Emit(OpCodes.Callvirt, IComparable_CompareTo);

                    // if prev and next are equal process next record
                    il.Emit(OpCodes.Brfalse, loopCollection);

                    // initialize function argument 1
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Stind_I1);

                    il.MarkLabel(nomore);
                }
            }
            else
            {
                if (0 == level)
                {
                    // fetch next record and return result
                    nomore = il.DefineLabel();

                    // read next record
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Callvirt, DbDataReader_Read);

                    // if no more records goto to nomore label
                    il.Emit(OpCodes.Brfalse_S, nomore);

                    // initialize function argument 1
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Stind_I1);

                    il.MarkLabel(nomore);
                }
            }
        }

        static void GenerateILCode(ref ushort vars, int level, Map map, ILGenerator il, DbDataReader reader)
        {
            ushort primary_key_value_idx = 0;
            ushort primary_key_value_second_idx = 0;

            var nomore = new Label(); var loopCollection = nomore;
            var properties = map.Properties.Where(t => typeof(Map).IsAssignableFrom(t.Value.GetType()))
                .Select(t => new { Property = t.Key, Map =  t.Value as Map }).ToList();

            foreach (var i in map.Properties.Where(t => t.Value.GetType() == typeof(string)))
            {
                GenerateILClass(ref vars, level + 1, il, reader, i.Value as string, i.Key.PropertyType, i.Key.GetSetMethod());
            }

            int primary_key_index = GetPrimaryKey(map, reader);// get primary key index
            if ((properties.Count != 0) && (0 != level || -1 != primary_key_index))
            {
                if (0 == level)
                {
                    // cycle by records break on primary key change
                    nomore = il.DefineLabel();
                    loopCollection = il.DefineLabel();

                    il.DeclareLocal(typeof(object));
                    primary_key_value_idx = vars; ++vars;

                    il.DeclareLocal(typeof(object));
                    primary_key_value_second_idx = vars; ++vars;

                    // get primary key value
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, primary_key_index);
                    il.Emit(OpCodes.Callvirt, DbDataReader_GetValue);
                    il.Emit(OpCodes.Stloc, primary_key_value_second_idx);

                    il.MarkLabel(loopCollection);

                    il.Emit(OpCodes.Ldloc, primary_key_value_second_idx);
                    il.Emit(OpCodes.Stloc, primary_key_value_idx);
                }

                foreach (var p in properties)
                    GenerateILCollection(ref vars, level + 1, p.Map, il, reader, p.Property.PropertyType, p.Property.GetGetMethod(), p.Property.GetSetMethod());

                if (0 == level)
                {
                    // cycle by records break on primary key change
                    // read next record
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Callvirt, DbDataReader_Read);

                    // if no more records goto to nomore label
                    il.Emit(OpCodes.Brfalse_S, nomore);

                    // get next record primary key value
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, primary_key_index);
                    il.Emit(OpCodes.Callvirt, DbDataReader_GetValue);

                    // store value for next loop
                    il.Emit(OpCodes.Stloc, primary_key_value_second_idx);
                    il.Emit(OpCodes.Ldloc, primary_key_value_second_idx);

                    // save into IComparable variable with cast???
                    // compare prev and next record primary key
                    il.Emit(OpCodes.Isinst, typeof(System.IComparable));
                    il.Emit(OpCodes.Ldloc, primary_key_value_idx);
                    il.Emit(OpCodes.Callvirt, IComparable_CompareTo);

                    // if prev and next are equal process next record
                    il.Emit(OpCodes.Brfalse, loopCollection);

                    // initialize function argument 1
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Stind_I1);

                    il.MarkLabel(nomore);
                }
            }
            else
            {
                if (0 == level)
                {
                    // fetch next record and return result
                    nomore = il.DefineLabel();

                    // read next record
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Callvirt, DbDataReader_Read);

                    // if no more records goto to nomore label
                    il.Emit(OpCodes.Brfalse_S, nomore);

                    // initialize function argument 1
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Stind_I1);

                    il.MarkLabel(nomore);
                }
            }
        }

        static void GenerateILCollection(ref ushort vars, int level, string namePrefix, string[,] commandToObjectMap, ILGenerator il, DbDataReader reader, Type collectionType, object getter, object setter)
        {
            int index = GetPrimaryKey(namePrefix, commandToObjectMap, reader);

            Type collectionInnerType = collectionType.ImplementGenericCollection();
            Label rowNotContainObjectData = il.DefineLabel();
            if (-1 != index)
            {
                // check to see if sub record exists, record contain object data
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, index);
                il.Emit(OpCodes.Callvirt, DbDataReader_IsDBNull);
                il.Emit(OpCodes.Brtrue, rowNotContainObjectData);
            }

            il.DeclareLocal(null == collectionInnerType ? collectionType : collectionInnerType); // Address
            ushort address_idx = vars; ++vars;

            if (null == collectionInnerType)
            {
                // not a collection, a property from object type

                // create new object Address and store to local
                il.Emit(OpCodes.Newobj, collectionType
                    .GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc, address_idx);

                // set property to instance of new Address object
                il.Emit(OpCodes.Dup);
                il.Emit(OpCodes.Ldloc, address_idx);
                if (setter is MethodInfo)
                    il.Emit(OpCodes.Callvirt, (MethodInfo)setter);
                else
                    il.Emit(OpCodes.Stfld, (FieldInfo)setter);

                il.Emit(OpCodes.Ldloc, address_idx); // set object Address as current into stack

                // load Address object properties and fields with values
                GenerateILCode(ref vars, level + 1, namePrefix, commandToObjectMap, il, reader, collectionType);

                il.Emit(OpCodes.Pop); // unload object Address from stack
            }
            else
            {
                if (-1 != index)
                {
                    // with this section ensure not to load same object twais
                    Label goLoadData = il.DefineLabel();

                    il.DeclareLocal(typeof(object));
                    ushort collection_pk_0_idx = vars; ++vars;
                    il.DeclareLocal(typeof(object));
                    ushort collection_pk_1_idx = vars; ++vars;

                    // read pk value
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, index);
                    il.Emit(OpCodes.Callvirt, DbDataReader_GetValue);
                    il.Emit(OpCodes.Stloc, collection_pk_1_idx);

                    // check is null
                    il.Emit(OpCodes.Ldnull);
                    il.Emit(OpCodes.Ldloc, collection_pk_0_idx);
                    il.Emit(OpCodes.Ceq);
                    il.Emit(OpCodes.Brtrue_S, goLoadData);

                    il.Emit(OpCodes.Ldloc, collection_pk_0_idx);
                    il.Emit(OpCodes.Isinst, typeof(System.IComparable));
                    il.Emit(OpCodes.Ldloc, collection_pk_1_idx);
                    il.Emit(OpCodes.Callvirt, IComparable_CompareTo);

                    il.Emit(OpCodes.Ldc_I4, -1); // резултата от IComparable_CompareTo е >= 0
                    il.Emit(OpCodes.Cgt);
                    il.Emit(OpCodes.Brtrue, rowNotContainObjectData);

                    il.MarkLabel(goLoadData);
                    il.Emit(OpCodes.Ldloc, collection_pk_1_idx);
                    il.Emit(OpCodes.Stloc, collection_pk_0_idx);
                }

                Label notNullCollection = il.DefineLabel();
                // define local variables
                il.DeclareLocal(collectionType);// List<Address>
                ushort list_address_idx = vars; ++vars;

                // load Object.Addresses into local List<Address>
                il.Emit(OpCodes.Dup);
                if (setter is MethodInfo)
                    il.Emit(OpCodes.Callvirt, (MethodInfo)getter);
                else
                    il.Emit(OpCodes.Ldfld, (FieldInfo)getter);
                il.Emit(OpCodes.Stloc, list_address_idx);

                // check local local List<Address> is null
                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Ldloc, list_address_idx);
                il.Emit(OpCodes.Ceq);

                // if is not null goto ...
                il.Emit(OpCodes.Brfalse, notNullCollection);

                // local List<Address> is null create and store into local
                il.Emit(OpCodes.Newobj, typeof(System.Collections.Generic.List<>)
                    .MakeGenericType(new Type[] { collectionInnerType })
                    .GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc, list_address_idx);

                // Object.Addresses to local List<Address>
                il.Emit(OpCodes.Dup);
                il.Emit(OpCodes.Ldloc, list_address_idx);
                if (setter is MethodInfo)
                    il.Emit(OpCodes.Callvirt, (MethodInfo)setter);
                else
                    il.Emit(OpCodes.Stfld, (FieldInfo)setter);

                il.MarkLabel(notNullCollection);

                // create new object Address and store to local
                il.Emit(OpCodes.Newobj, collectionInnerType
                    .GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc, address_idx);


                // add new object Address to collection
                il.Emit(OpCodes.Ldloc, list_address_idx);
                il.Emit(OpCodes.Ldloc, address_idx);
                il.Emit(OpCodes.Callvirt, collectionType
                    .GetMethod("Add"));

                il.Emit(OpCodes.Ldloc, address_idx); // set object Address as current into stack

                // load Address object properties and fields with values
                GenerateILCode(ref vars, level + 1, namePrefix, commandToObjectMap, il, reader, collectionInnerType);

                il.Emit(OpCodes.Pop); // unload object Address from stack
            }

            il.MarkLabel(rowNotContainObjectData);
        }

        static void GenerateILCollection(ref ushort vars, int level, Map map, ILGenerator il, DbDataReader reader, Type collectionType, object getter, object setter)
        {
            int index = GetPrimaryKey(map, reader);

            Type collectionInnerType = collectionType.ImplementGenericCollection();
            Label rowNotContainObjectData = il.DefineLabel();
            if (-1 != index)
            {
                // check to see if sub record exists, record contain object data
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, index);
                il.Emit(OpCodes.Callvirt, DbDataReader_IsDBNull);
                il.Emit(OpCodes.Brtrue, rowNotContainObjectData);
            }

            il.DeclareLocal(null == collectionInnerType ? collectionType : collectionInnerType); // Address
            ushort address_idx = vars; ++vars;

            if (null == collectionInnerType)
            {
                // not a collection, a property from object type

                // create new object Address and store to local
                il.Emit(OpCodes.Newobj, collectionType
                    .GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc, address_idx);

                // set property to instance of new Address object
                il.Emit(OpCodes.Dup);
                il.Emit(OpCodes.Ldloc, address_idx);
                if (setter is MethodInfo)
                    il.Emit(OpCodes.Callvirt, (MethodInfo)setter);
                else
                    il.Emit(OpCodes.Stfld, (FieldInfo)setter);

                il.Emit(OpCodes.Ldloc, address_idx); // set object Address as current into stack

                // load Address object properties and fields with values
                GenerateILCode(ref vars, level + 1, map, il, reader);

                il.Emit(OpCodes.Pop); // unload object Address from stack
            }
            else
            {
                if (-1 != index)
                {
                    // with this section ensure not to load same object twais
                    Label goLoadData = il.DefineLabel();

                    il.DeclareLocal(typeof(object));
                    ushort collection_pk_0_idx = vars; ++vars;
                    il.DeclareLocal(typeof(object));
                    ushort collection_pk_1_idx = vars; ++vars;

                    // read pk value
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, index);
                    il.Emit(OpCodes.Callvirt, DbDataReader_GetValue);
                    il.Emit(OpCodes.Stloc, collection_pk_1_idx);

                    // check is null
                    il.Emit(OpCodes.Ldnull);
                    il.Emit(OpCodes.Ldloc, collection_pk_0_idx);
                    il.Emit(OpCodes.Ceq);
                    il.Emit(OpCodes.Brtrue_S, goLoadData);

                    il.Emit(OpCodes.Ldloc, collection_pk_0_idx);
                    il.Emit(OpCodes.Isinst, typeof(System.IComparable));
                    il.Emit(OpCodes.Ldloc, collection_pk_1_idx);
                    il.Emit(OpCodes.Callvirt, IComparable_CompareTo);

                    il.Emit(OpCodes.Ldc_I4, -1); // резултата от IComparable_CompareTo е >= 0
                    il.Emit(OpCodes.Cgt);
                    il.Emit(OpCodes.Brtrue, rowNotContainObjectData);

                    il.MarkLabel(goLoadData);
                    il.Emit(OpCodes.Ldloc, collection_pk_1_idx);
                    il.Emit(OpCodes.Stloc, collection_pk_0_idx);
                }

                Label notNullCollection = il.DefineLabel();
                // define local variables
                il.DeclareLocal(collectionType);// List<Address>
                ushort list_address_idx = vars; ++vars;

                // load Object.Addresses into local List<Address>
                il.Emit(OpCodes.Dup);
                if (setter is MethodInfo)
                    il.Emit(OpCodes.Callvirt, (MethodInfo)getter);
                else
                    il.Emit(OpCodes.Ldfld, (FieldInfo)getter);
                il.Emit(OpCodes.Stloc, list_address_idx);

                // check local local List<Address> is null
                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Ldloc, list_address_idx);
                il.Emit(OpCodes.Ceq);

                // if is not null goto ...
                il.Emit(OpCodes.Brfalse, notNullCollection);

                // local List<Address> is null create and store into local
                il.Emit(OpCodes.Newobj, typeof(System.Collections.Generic.List<>)
                    .MakeGenericType(new Type[] { collectionInnerType })
                    .GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc, list_address_idx);

                // Object.Addresses to local List<Address>
                il.Emit(OpCodes.Dup);
                il.Emit(OpCodes.Ldloc, list_address_idx);
                if (setter is MethodInfo)
                    il.Emit(OpCodes.Callvirt, (MethodInfo)setter);
                else
                    il.Emit(OpCodes.Stfld, (FieldInfo)setter);

                il.MarkLabel(notNullCollection);

                // create new object Address and store to local
                il.Emit(OpCodes.Newobj, collectionInnerType
                    .GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc, address_idx);


                // add new object Address to collection
                il.Emit(OpCodes.Ldloc, list_address_idx);
                il.Emit(OpCodes.Ldloc, address_idx);
                il.Emit(OpCodes.Callvirt, collectionType
                    .GetMethod("Add"));

                il.Emit(OpCodes.Ldloc, address_idx); // set object Address as current into stack

                // load Address object properties and fields with values
                GenerateILCode(ref vars, level + 1, map, il, reader);

                il.Emit(OpCodes.Pop); // unload object Address from stack
            }

            il.MarkLabel(rowNotContainObjectData);
        }

        static bool GenerateILClass(ref ushort vars, int level, string namePrefix, string[,] commandToObjectMap, ILGenerator il, DbDataReader reader, string name, Type type, object setter, bool skip = true)
        {
            int index = -1;
            Type nullableMemberType = null; Type collectionType = null;
            Type memberType = Nullable.GetUnderlyingType(type);
            if (null == memberType)
            {
                memberType = type;
                collectionType = type.ImplementGenericCollection();
            }
            else
            {
                nullableMemberType = type;
            }

            bool b = null == collectionType && (memberType.IsValueType || memberType == typeof(string));
            if (b)
            {
                if (-1 == (index = GetFieldIndex(namePrefix + name, name, commandToObjectMap, reader)))
                    return false;

                GenerateILCode(ref vars, level + 1, il, index, nullableMemberType, memberType, reader.GetFieldType(index) != memberType, setter);
            }
            else
            {
                return skip;
            }
            return false;
        }

        static bool GenerateILClass(ref ushort vars, int level, ILGenerator il, DbDataReader reader, string name, Type type, object setter, bool skip = true)
        {
            int index = -1;
            Type nullableMemberType = null; Type collectionType = null;
            Type memberType = Nullable.GetUnderlyingType(type);
            if (null == memberType)
            {
                memberType = type;
                collectionType = type.ImplementGenericCollection();
            }
            else
            {
                nullableMemberType = type;
            }

            bool b = null == collectionType && (memberType.IsValueType || memberType == typeof(string));
            if (b)
            {
                if (-1 == (index = GetFieldIndex(name, reader)))
                    return false;

                GenerateILCode(ref vars, level + 1, il, index, nullableMemberType, memberType, reader.GetFieldType(index) != memberType, setter);
            }
            else
            {
                return skip;
            }
            return false;
        }

        static void GenerateILCode(ref ushort vars, int level, ILGenerator il, int index, Type nullableMemberType, Type memberType, bool notEqualTypes, object setter)
        {
            Label isDbNullLabel = il.DefineLabel();
            Label endLabel = il.DefineLabel();

            il.Emit(OpCodes.Dup); // stack is now [target] [target]

            il.Emit(OpCodes.Ldarg_0); // stack is now [target] [target] [reader]
            il.Emit(OpCodes.Ldc_I4, index); // stack is now [target] [target] [reader] [index]
            il.Emit(OpCodes.Callvirt, DbDataReader_IsDBNull); // stack is now [target] [target] [is DBNull]

            il.Emit(OpCodes.Brtrue_S, isDbNullLabel); // stack is now [target] [target]

            il.Emit(OpCodes.Ldarg_0); // stack is now [target] [target] [reader]
            il.Emit(OpCodes.Ldc_I4, index); // stack is now [target] [target] [reader] [index]
            il.Emit(OpCodes.Callvirt, DbDataReader_GetValue); // stack is now [target] [target] [object value]

            if (notEqualTypes)
            {
                il.Emit(OpCodes.Ldtoken, memberType); // stack is now [target] [target] [object value] [Type token]
                il.EmitCall(OpCodes.Call, Type_GetTypeFromHandle, null); // stack is now [target] [target] [object value] [Type]
                il.EmitCall(OpCodes.Call, Convert_ChangeType, null); // stack is now [target] [target] [object value]
            }

            il.Emit(OpCodes.Unbox_Any, memberType); //[target] [target] [value]

            if (null != nullableMemberType)
            {
                il.Emit(OpCodes.Newobj, nullableMemberType.GetConstructor(new[] { memberType }));//[target] [target] [nullable value]
            }

            if (setter is MethodInfo)
                il.Emit(OpCodes.Callvirt, (MethodInfo)setter); // stack is now [target]
            else
                il.Emit(OpCodes.Stfld, (FieldInfo)setter); // stack is now [target]

            il.Emit(OpCodes.Br, endLabel); // stack is now [target]

            il.MarkLabel(isDbNullLabel); // stack is now [target] [target]

            il.Emit(OpCodes.Pop); // stack is now [target]

            il.MarkLabel(endLabel); // stack is now [target]
        }

        static int GetPrimaryKey(string prefix, string[,] commandToObjectMap, DbDataReader reader)
        {
            string primaryKeyName;
            string primaryKeyFieldName = string.Empty;

            if (string.IsNullOrEmpty(prefix))
            {
                primaryKeyName = "PrimaryKey";
                primaryKeyFieldName = "Id";
            }
            else
            {
                primaryKeyName = prefix + "PrimaryKey";
                primaryKeyFieldName = prefix + "Id";
            }

            for (int i = 0, l = commandToObjectMap.Length / 2; i < l; i++)
                if (0 == string.Compare(primaryKeyName, commandToObjectMap[i, 0], StringComparison.InvariantCulture))
                {
                    primaryKeyFieldName = commandToObjectMap[i, 1];
                    break;
                }

            int index = -1;
            try
            {
                index = reader.GetOrdinal(primaryKeyFieldName);
            }
            catch (IndexOutOfRangeException)
            { }

            return index;
        }

        static int GetPrimaryKey(Map map, DbDataReader reader)
        {
            int index = -1;
            try
            {
                index = reader.GetOrdinal(map.PrimaryKeyColumnName);
            }
            catch (IndexOutOfRangeException)
            { }

            return index;
        }

        static int GetFieldIndex(string mapName, string fieldName, string[,] commandToObjectMap, DbDataReader reader)
        {
            string searchFieldName = mapName;

            if (null != commandToObjectMap)
                for (int i = 0, l = commandToObjectMap.Length / 2; i < l; i++)
                    if (0 == string.Compare(mapName, commandToObjectMap[i, 0], StringComparison.InvariantCulture))
                    {
                        searchFieldName = commandToObjectMap[i, 1];
                        break;
                    }

            int index = -1;
            try
            {
                index = reader.GetOrdinal(searchFieldName);
            }
            catch (IndexOutOfRangeException)
            {
                try
                {
                    index = reader.GetOrdinal(fieldName);
                }
                catch (IndexOutOfRangeException)
                { }
            }

            return index;
        }

        static int GetFieldIndex(string name, DbDataReader reader)
        {
            int index = -1;
            try
            {
                index = reader.GetOrdinal(name);
            }
            catch (IndexOutOfRangeException)
            { }
            return index;
        }
    }
}
