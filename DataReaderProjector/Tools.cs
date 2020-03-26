using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataReaderProjector
{
    public static class Tools
    {
        public static Type ImplementGenericCollection(this Type o)
        {
            Type f = null;
            if (!o.IsValueType && o.IsGenericType)
            {
                if (o.GetGenericTypeDefinition() == typeof(List<>) || o.GetGenericTypeDefinition() == typeof(ICollection<>))
                    f = o.GetGenericArguments()[0];
            }
            return f;
        }
    }
}