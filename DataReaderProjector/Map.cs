using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace DataReaderProjector
{
    public abstract class Map
    {
        public abstract Type Type
        {
            get;
            protected set;
        }

        public abstract string Name
        {
            get;
            protected set;
        }

        public abstract bool IsCollection
        {
            get;
            protected set;
        }

        public abstract string PrimaryKeyColumnName
        {
            get;
            protected set;
        }

        public abstract IEnumerable<KeyValuePair<PropertyInfo, object>> Properties
        {
            get;
        }
    }

    public class Map<T> : Map where T : class
    {
        readonly List<KeyValuePair<PropertyInfo, object>> properties = new List<KeyValuePair<PropertyInfo, object>>();

        public Map()
        {
            Type = typeof(T);
            Name = Type.Name;
            if (!Type.IsClass || typeof(string) == Type)
            {
                throw new ArgumentException("Invalid type argument.");
            }

            if (typeof(IEnumerable).IsAssignableFrom(Type))
            {
                throw new ArgumentException("Invalid type argument.");
            }
        }

        public override IEnumerable<KeyValuePair<PropertyInfo, object>> Properties
        {
            get
            {
                return properties;
            }
        }

        public override Type Type
        {
            get;
            protected set;
        }

        public override string Name
        {
            get;
            protected set;
        }

        public override bool IsCollection
        {
            get;
            protected set;
        }

        public override string PrimaryKeyColumnName
        {
            get;
            protected set;
        }

        public Map<T> PrimaryKey(string columnName = null)
        {
            this.PrimaryKeyColumnName = columnName ?? string.Empty;
            return this;
        }

        public Map<T> Property<TProperty>(Expression<Func<T,TProperty>> expression, string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName))
            {
                throw new ArgumentNullException(nameof(columnName));
            }
            if (expression == null)
            {
                throw new ArgumentNullException(nameof(expression));
            }

            if (!(expression.Body is MemberExpression e) || e.Member.MemberType != MemberTypes.Property)
            {
                throw new ArgumentException("Invalid type argument.");
            }

            if (e.Member.Name.Equals("Id", StringComparison.InvariantCultureIgnoreCase))
            {
                this.PrimaryKey(columnName);
            }

            properties.Add(new KeyValuePair<PropertyInfo, object>((PropertyInfo)e.Member, columnName));

            return this;
        }

        public Map<T> Object<TProperty>(Expression<Func<T,TProperty>> expression, Action<Map<TProperty>> action)
            where TProperty : class
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }
            if (expression == null)
            {
                throw new ArgumentNullException(nameof(expression));
            }

            var t = typeof(TProperty);
            if (!(expression.Body is MemberExpression e) || e.Member.MemberType != MemberTypes.Property || t == typeof(string) || typeof(IEnumerable).IsAssignableFrom(t))
            {
                throw new ArgumentException("Invalid type argument.");
            }

            var map = new Map<TProperty>();
            action(map);
            properties.Add(new KeyValuePair<PropertyInfo, object>((PropertyInfo)e.Member, map));

            return this;
        }

        public Map<T> Collection<TProperty>(Expression<Func<T,IEnumerable<TProperty>>> expression, Action<Map<TProperty>> action)
            where TProperty : class
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }
            if (expression == null)
            {
                throw new ArgumentNullException(nameof(expression));
            }

            var t = typeof(TProperty);
            if (!(expression.Body is MemberExpression e) || e.Member.MemberType != MemberTypes.Property || t == typeof(string))
            {
                throw new ArgumentException("Invalid type argument.");
            }

            var map = new Map<TProperty>() { IsCollection = true };
            action(map);
            properties.Add(new KeyValuePair<PropertyInfo, object>((PropertyInfo)e.Member, map));

            return this;
        }
    }
}
