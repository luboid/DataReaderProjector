using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;

namespace DataReaderProjector.Dynamic
{
    class DynObj : DynamicObject
    {
        IDictionary<string, object> data;

        public static DynObj Create(IDictionary<string, object> data)
        {
            return new DynObj { data = data };
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            data[binder.Name] = value;
            return true;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return data.TryGetValue(binder.Name, out result);
        }
    }
}