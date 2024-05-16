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
         id, brio, paths, refs)
       values (
         @id, @brio, @paths, @refs::jsonb)").prepare

//    pathRefInsert = conn.sql(
//      "insert into pathRef
//         (source, path_, target)
//       values
//         (@source, @path, @target)").prepare
  }

  **
  ** Close the connection to Postgres
  **
  Void close()
  {
    recInsert.close
    //pathRefInsert.close

    recInsert = null
    //pathRefInsert = null

    // Close the connection
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
      "id":     rec.id,
      "brio":   BrioWriter.valToBuf(dict),
      "paths":  rec.paths,
      "refs":   JsonOutStream.writeJsonToStr(rec.refs),
    ])

//    rec.pathRefs.each |target, path|
//    {
//      pathRefInsert.execute([
//        "source": rec.id.id,
//        "path":   path,
//        "target": target.id
//      ])
//    }

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
  //private Statement? pathRefInsert
}

