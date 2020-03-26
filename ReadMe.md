# Simple not opinioned ORM

***When you have an old database with mismatch naming rules*** 

> SQL (oracle dialect) which define data set, need to be ordered by primary keys 
```
select * from (
    select t.csuniq, t.csname, t.egn_ekpou,
           a.uniqid cauniq, a.cakind, a.cacity, a.capk,
           z.hivalue, z.hivdescr,
           pt.hivalue ptid, pt.hivdescr ptdescr,
           p.uniqid cpuniq, p.cpkind, p.cpareacode, p.cpphone, p.cpcontact
      from (select hivalue, hivdescr
              from bshintvalues
             where hiuniq = clients.typeaddr) z,
           (select hivalue, hivdescr
              from bshintvalues
             where hiuniq = clients.phonetype) pt,
           csphones p,
           csaddresses a, cscommon t
     where z.hivalue(+) = a.cakind
       and pt.hivalue(+) = p.cpkind
       and a.csuniq(+) = t.csuniq
       and p.csuniq(+) = t.csuniq
     order by t.csuniq, cauniq, cpuniq
) where rownum <= 20000
```
> Map projected Customer class to dataset fields
```
var map = new Map<Customer>()
    .Property(m => m.Id, "csuniq")
    .Property(m => m.Name, "csname")
    .Property(m => m.EGN, "egn_ekpou")
    .Collection(m => m.Addresses, (address) =>
    {
        address
        .Property(a => a.Id, "cauniq")
        .Property(a => a.Kind, "cakind")
        .Property(a => a.City, "cacity")
        .Property(a => a.Zip, "capk")
        .Object(a => a.Type, (typ) =>
        {
            typ
            .Property(tp => tp.Id, "hivalue")
            .Property(tp => tp.Description, "hivdescr");
        });
    })
    .Collection(m => m.Phones, (phone) =>
    {
        phone
        .Property(p => p.Id, "cpuniq")
        .Property(p => p.Kind, "cpkind")
        .Property(p => p.AreaCode, "cpareacode")
        .Property(p => p.Number, "cpphone")
        .Property(p => p.Contact, "cpcontact")
        .Object(p => p.Type, (typ) =>
        {
            typ
            .Property(tp => tp.Id, "ptid")
            .Property(tp => tp.Description, "ptdescr");
        });
    });
```
> Now execute SQL and get Customers
```
using var conn = CreateConnection();

conn.Open();

foreach (var c in conn.Enumerate(CommandType.Text, commandText, null, map))
```
