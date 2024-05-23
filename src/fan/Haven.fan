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
         strs, nums, units,
         bools, uris,
         dates, times, dateTimes)
       values (
         @id, @brio, @paths,
         @strs::jsonb, @nums::jsonb, @units::jsonb,
         @bools::jsonb, @uris::jsonb,
         @dates::jsonb, @times::jsonb, @dateTimes::jsonb)"
     ).prepare

    pathRefInsert = conn.sql(
      "insert into path_ref
         (source, path_, target)
       values
         (@source, @path, @target)"
     ).prepare

    refTagInsert = conn.sql(
      "insert into ref_tag (name) values (@name)"
     ).prepare

    byIdSelect = conn.sql(
      "select brio from rec where id = @id"
    ).prepare;

    // load ref tags
    stmt := conn.sql("select name from ref_tag")
    stmt.query.each |r|
    {
      name := r->name
      refTagSet[name] = name
    }
    stmt.close
  }

  **
  ** Close the connection to Postgres
  **
  Void close()
  {
    recInsert.close
    pathRefInsert.close
    refTagInsert.close
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
      "strs":      (rec.strs      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.strs),
      "nums":      (rec.nums      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.nums),
      "units":     (rec.units     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.units),
      "bools":     (rec.bools     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.bools),
      "uris":      (rec.uris      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.uris),
      "dates":     (rec.dates     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dates),
      "times":     (rec.times     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.times),
      "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes)
    ])

    rec.refs.each |targets, path|
    {
      // update ref tags if need be
      n := path.indexr(".")
      lastTag := (n == null) ? path : path[(n+1)..-1]
      if (!refTagSet.containsKey(lastTag))
      {
        refTagInsert.execute(["name": lastTag])
        refTagSet[lastTag] = lastTag
      }

      // insert path refs
      targets.each | target |
      {
        pathRefInsert.execute([
          "source": rec.id,
          "path":   path,
          "target": target
        ])
      }
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

  **
  ** select by list of ids
  **
  Dict[] selectByIds(Ref[] ids)
  {
    res := Dict[,]
    if (ids.isEmpty)
      return res

    sql := StrBuf()
    sql.add("select brio from rec where id in (")
    params := Str:Obj?[:]
    ids.each |id, i|
    {
      if (i > 0)
        sql.add(", ")
      sql.add("@x$i")
      params.add("x$i", id.id)
    }
    sql.add(");")

    stmt := conn.sql(sql.toStr).prepare
    stmt.query(params).each |r|
    {
      Buf brio := r->brio
      res.add(BrioReader(brio.in).readDict)
    }
    stmt.close

    return res
  }

  **
  ** Execute a query
  **
  Dict[] select(Query q)
  {
    res := Dict[,]
    Str? prevId := null

    // TODO cache these?
    stmt := conn.sql(q.sql).prepare
    stmt.query(q.params).each |r|
    {
      idCol := r.col("id", false)

      // If there is no id column, then the resulset is not ordered
      if (idCol == null)
      {
        res.add(BrioReader(((Buf)r->brio).in).readDict)
      }
      // Otherwise, the result is ordered by id, so we track the previous id
      // to discard duplicate records.
      else
      {
        id := r.get(idCol)
        if (id != prevId)
        {
          res.add(BrioReader(((Buf)r->brio).in).readDict)
        }
        prevId = id
      }
    }
    stmt.close

    return res
  }

  ** make a List of dotted Paths ending in Refs, using the refTag whitelist
  internal Str[] refPaths(FilterPath fp)
  {
    result := Str[,]

    cur := Str[,]
    for (i := 0; i < fp.size; i++)
    {
      tag := fp.get(i)
      cur.add(tag)

      if (refTagSet.containsKey(tag))
      {
        result.add(cur.join("."))
        cur.clear
      }
    }

    if (!cur.isEmpty)
      result.add(cur.join("."))

    return result
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  // N.B. we use the connection internally in the test suite
  internal SqlConn? conn

  private Statement? recInsert
  private Statement? pathRefInsert
  private Statement? refTagInsert
  private Statement? byIdSelect

  // Always mirrors the records in the ref_tag table
  private Str:Str refTagSet := Str:Str[:]
}

