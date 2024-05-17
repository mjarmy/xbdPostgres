//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using sql
using haystack
using util

**
** Haven stores and queries Dicts in a Postgres database
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

    recInsert = conn.sql(
      "insert into rec (
         id, brio, paths,
         refs, strs, nums, units,
         bools, uris,
         dates, times, dateTimes)
       values (
         @id, @brio, @paths,
         @refs::jsonb, @strs::jsonb, @nums::jsonb, @units::jsonb,
         @bools::jsonb, @uris::jsonb,
         @dates::jsonb, @times::jsonb, @dateTimes::jsonb)"
     ).prepare

    pathRefInsert = conn.sql(
      "insert into path_ref
         (source, path_, target)
       values
         (@source, @path, @target)"
     ).prepare

    byIdSelect = conn.sql(
      "select brio from rec where id = @id"
    ).prepare;
  }

  **
  ** Close the connection to Postgres
  **
  Void close()
  {
    recInsert.close
    pathRefInsert.close
    byIdSelect.close

    conn.close
    conn = null
  }

  **
  ** Insert a Rec
  **
  Void insertRec(Dict dict)
  {
    rec := Rec.fromDict(dict)

    recInsert.execute([
      "id":        rec.id,
      "brio":      BrioWriter.valToBuf(dict),
      "paths":     rec.paths,
      "refs":      JsonOutStream.writeJsonToStr(rec.refs),
      "strs":      (rec.strs      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.strs),
      "nums":      (rec.nums      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.nums),
      "units":     (rec.units     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.units),
      "bools":     (rec.bools     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.bools),
      "uris":      (rec.uris      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.uris),
      "dates":     (rec.dates     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dates),
      "times":     (rec.times     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.times),
      "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes)
    ])

    rec.refs.each |target, path|
    {
      pathRefInsert.execute([
        "source": rec.id,
        "path":   path,
        "target": target
      ])
    }

    conn.commit
  }

  **
  ** select by id
  **
  Dict? selectById(Ref id)
  {
    rows := byIdSelect.query(["id": id.id])

    if (rows.isEmpty)
    {
      return null;
    }
    else
    {
      Buf brio := rows[0]->brio
      return BrioReader(brio.in).readDict
    }
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
//      result.add(BrioReader(((Buf)r->brio).in).readDict)
//    }
//    stmt.close
//
//    return result
//  }
//
//  **
//  ** Explain a select
//  **
//  Str[] explain(Str rawSql)
//  {
//    result := Str[,]
//
//    stmt := conn.sql(
//        "explain (analyze true, verbose true, buffers true) " +
//        rawSql)
//    stmt.query().each |row|
//    {
//      col := row.col("QUERY PLAN")
//      result.add(row[col])
//    }
//    stmt.close
//
//    return result
//  }

  private SqlConn? conn
  private Statement? recInsert
  private Statement? pathRefInsert
  private Statement? byIdSelect
}

