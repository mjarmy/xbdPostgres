//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using sql

****************************************************************
** Db
****************************************************************

class Db
{
  Void open(Str uri, Str? username, Str? password)
  {
    conn = SqlConn.open(uri, username, password)
    conn.autoCommit = false

    specInsert = conn.sql(
      "insert into spec (qname, inherits_from)
       values (@qname, @inheritsFrom)").prepare

    recInsert = conn.sql(
      "insert into rec (
         id, paths,
         values_, refs, units,
         spec)
       values (
         @id, @paths,
         @values::jsonb, @refs::jsonb, @units::jsonb,
         @spec)").prepare

    pathRefInsert = conn.sql(
      "insert into pathRef (rec_id, path_, ref_)
       values (@recId, @path, @ref)").prepare
  }

  Void close()
  {
    specInsert.close
    specInsert = null

    recInsert.close
    recInsert = null

    pathRefInsert.close
    pathRefInsert = null

    conn.close
    conn = null
  }

  Void insertSpec(Str qname, Str[] inheritsFrom)
  {
    specInsert.execute([
      "qname":qname,
      "inheritsFrom": inheritsFrom
    ])
    conn.commit
  }

  Void insertRec(DbRec rec)
  {
    echo("insertRec ${rec.id}")

    recInsert.execute([
      "id":     rec.id.id,
      "paths":  rec.paths,
      "values": JsonWriter.valToStr(rec.values),
      "refs":   JsonWriter.valToStr(rec.refs),
      "units":  JsonWriter.valToStr(rec.units),
      "spec":   rec.spec == null ? null : rec.spec.id,
    ])

    rec.pathRefs.each |r, p|
    {
      pathRefInsert.execute([
        "recId": rec.id.id,
        "path":  p,
        "ref":   r.id
      ])
    }

    conn.commit
  }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  private SqlConn? conn
  private Statement? specInsert
  private Statement? recInsert
  private Statement? pathRefInsert
}
