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
** Haven handles interaction with the Postgres database
**
class Haven
{
  **
  ** Open the connection to Postgres
  **
  Void open(Str uri, Str? username, Str? password)
  {
    conn = SqlConn.open(uri, username, password)
    conn.autoCommit = false

    specInsert = conn.sql(
      "insert into spec
         (qname, inherits_from)
       values
         (@qname, @inheritsFrom)").prepare

    recInsert = conn.sql(
      "insert into rec (
         id, brio, paths, hayson, spec)
       values (
         @id, @brio, @paths, @hayson::jsonb, @spec)").prepare

    pathRefInsert = conn.sql(
      "insert into pathRef
         (source, path_, target)
       values
         (@source, @path, @target)").prepare
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
  Void insertRec(Dict dict)
  {
    rec := Rec.fromDict(dict)
    echo("insertRec: ${rec.id}")

    recInsert.execute([
      "id":     rec.id.id,
      "brio":   BrioWriter.valToBuf(dict),
      "paths":  rec.paths,
      "hayson": JsonWriter.valToStr(rec.hayson),
      "spec":   rec.spec == null ? null : rec.spec.id,
    ])

    rec.pathRefs.each |target, path|
    {
      pathRefInsert.execute([
        "source": rec.id.id,
        "path":   path,
        "target": target.id
      ])
    }

    conn.commit
  }

//  **
//  ** Execute a query
//  **
//  Dict[] select(Query q)
//  {
//    result := Dict[,]
//
//    // TODO cache these?
//    stmt := conn.sql(q.sql).prepare
//    stmt.query(q.params).each |r|
//    {
//      Str? spec := r->spec
//      result.add(Rec(
//       Ref.fromStr(r->id),
//       r->paths,
//       JsonReader(((Str)r->values_).in).readVal,
//       JsonReader(((Str)r->refs)   .in).readVal,
//       JsonReader(((Str)r->units)  .in).readVal,
//       spec == null ? null : Ref.fromStr(spec)))
//    }
//    stmt.close
//
//    return result
//  }

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

