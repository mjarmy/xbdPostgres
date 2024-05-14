//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using sql

**
** Storehouse handles interaction with the Postgres database
**
class Storehouse
{
  **
  ** Open the connection to Postgres
  **
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

  **
  ** Close the connection to Postgres
  **
  Void close()
  {
    // TODO probably don't need this?
    specInsert.close
    specInsert = null

    recInsert.close
    recInsert = null

    pathRefInsert.close
    pathRefInsert = null

    // Close the connection
    conn.close
    conn = null
  }

  **
  ** Insert a Spec
  **
  Void insertSpec(Str qname, Str[] inheritsFrom)
  {
    specInsert.execute([
      "qname":qname,
      "inheritsFrom": inheritsFrom
    ])
    conn.commit
  }

  **
  ** Insert a Rec
  **
  Void insertRec(DbRec rec)
  {
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

  **
  ** Execute a query
  **
  DbRec[] select(Query q)
  {
    result := DbRec[,]

    // TODO cache these?
    stmt := conn.sql(q.sql).prepare
    stmt.query(q.params).each |r|
    {
      Str? spec := r->spec
      result.add(DbRec(
       Ref.fromStr(r->id),
       r->paths,
       JsonReader(((Str)r->values_).in).readVal,
       JsonReader(((Str)r->refs)   .in).readVal,
       JsonReader(((Str)r->units)  .in).readVal,
       spec == null ? null : Ref.fromStr(spec)))
    }
    stmt.close

    return result
  }

  **
  ** Explain a select
  **
  Str[] explain(Str rawSql)
  {
    result := Str[,]

    stmt := conn.sql(
        "explain (analyze true, verbose true, buffers true) " +
        rawSql)
    stmt.query().each |row|
    {
      col := row.col("QUERY PLAN")
      result.add(row[col])
    }
    stmt.close

    return result
  }

  private SqlConn? conn
  private Statement? specInsert
  private Statement? recInsert
  private Statement? pathRefInsert
}
