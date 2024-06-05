//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using concurrent
using haystack
using sql
using util

**
** Haven stores and queries Dicts in a Postgres database
**
class Haven
{
  new make(SqlConnPool pool)
  {
    this.pool = pool

    // load ref tags
    pool.execute(|SqlConn conn| {
      stmt := conn.sql("select name from ref_tag")
      stmt.query.each |r|
      {
        name := r->name
        refTags[name] = name
      }
      stmt.close
    })
  }

//////////////////////////////////////////////////////////////////////////
// Reads
//////////////////////////////////////////////////////////////////////////

  **
  ** Read single record by its id
  **
  Dict? readById(Ref? id, Bool checked := true)
  {
    Dict? result := null

    pool.execute(|SqlConn conn| {
      stmt := fetch(conn, "#selectById", |->Str| { selectById })
      rows := stmt.query(["id": id.id])
      result = doReadSingle(rows, checked, id.toStr)
    })

    return result
  }

  **
  ** Read a single value from a ResultSet
  **
  private Dict? doReadSingle(sql::Row[] rows, Bool checked, Str errMsg)
  {
    if (rows.isEmpty)
    {
      if (checked)
        throw UnknownRecErr(errMsg)
      else
        return null
    }
    else
    {
      Buf brio := rows[0]->brio
      return BrioReader(brio.in).readDict
    }
  }

  ** Make a List of dotted Paths ending in Refs, using the refTag whitelist.
  ** This will be used to construct a series of inner joins in
  ** QueryBuilder.visitLeaf().
  internal Str[] refPaths(FilterPath fp)
  {
    result := Str[,]

    cur := Str[,]
    for (i := 0; i < fp.size; i++)
    {
      tag := fp.get(i)
      cur.add(tag)

      if (refTags.containsKey(tag))
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
// Prepared Statements
//////////////////////////////////////////////////////////////////////////

  private Statement fetch(SqlConn conn, Str key, |->Str| sqlFunc)
  {
    if (conn.stash.containsKey(key))
    {
      echo("fetch: found $key")
      return conn.stash[key]
    }
    else
    {
      echo("fetch: ------------> PREPARING $key")
      stmt := conn.sql(sqlFunc()).prepare
      conn.stash[key] = stmt
      return stmt
    }
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  const Str selectById :=
    "select brio from rec where id = @id"

  private SqlConnPool pool

  ** refTags is a Set that mirrors the records in the ref_tag table.
  private ConcurrentMap refTags := ConcurrentMap()
}


//  **
//  ** Open the connection to Postgres
//  **
//  static Haven open(Str uri, Str? username, Str? password)
//  {
//    return Haven(uri, username, password)
//  }
//
//  private new make(Str uri, Str? username, Str? password)
//  {
//    this.conn = SqlConn.open(uri, username, password)
//    conn.autoCommit = false
//
//    this.specInsert = conn.sql(
//      "insert into spec
//         (qname, inherits_from)
//       values
//         (@qname, @inheritsFrom)").prepare
//
//    this.recInsert = conn.sql(
//      "insert into rec (
//         id, brio, paths,
//         strs, nums, units,
//         bools, uris,
//         dates, times, dateTimes,
//         spec)
//       values (
//         @id, @brio, @paths,
//         @strs::jsonb, @nums::jsonb, @units::jsonb,
//         @bools::jsonb, @uris::jsonb,
//         @dates::jsonb, @times::jsonb, @dateTimes::jsonb,
//         @spec)"
//     ).prepare
//
//    this.pathRefInsert = conn.sql(
//      "insert into path_ref
//         (source, path_, target)
//       values
//         (@source, @path, @target)"
//     ).prepare
//
//    this.refTagInsert = conn.sql(
//      "insert into ref_tag (name) values (@name)"
//     ).prepare
//
//    this.byIdSelect = conn.sql(
//      "select brio from rec where id = @id"
//    ).prepare
//
//    this.recUpdate = conn.sql(
//      "update rec set
//         brio      = @brio,
//         paths     = @paths,
//         strs      = @strs::jsonb,
//         nums      = @nums::jsonb,
//         units     = @units::jsonb,
//         bools     = @bools ::jsonb,
//         uris      = @uris::jsonb,
//         dates     = @dates ::jsonb,
//         times     = @times ::jsonb,
//         dateTimes = @dateTimes::jsonb,
//         spec      = @spec
//       where id = @id"
//    ).prepare
//
//    this.recDelete = conn.sql(
//      "delete from rec where id = @id"
//    ).prepare
//
//    this.pathRefDelete = conn.sql(
//      "delete from path_ref where source = @source"
//    ).prepare
//
//    // load ref tags
//    stmt := conn.sql("select name from ref_tag")
//    stmt.query.each |r|
//    {
//      name := r->name
//      refTags[name] = name
//    }
//    stmt.close
//  }
//
//  **
//  ** Close the connection to Postgres
//  **
//  Void close()
//  {
//    specInsert.close
//    recInsert.close
//    pathRefInsert.close
//    refTagInsert.close
//    byIdSelect.close
//    recUpdate.close
//    recDelete.close
//    pathRefDelete.close
//
//    conn.close
//    conn = null
//  }
//
////////////////////////////////////////////////////////////////////////////
//// Reads
////////////////////////////////////////////////////////////////////////////
//
//  **
//  ** Read single record by its id
//  **
//  Dict? readById(Ref? id, Bool checked := true)
//  {
//    rows := byIdSelect.query(["id": id.id])
//    return doReadSingle(rows, checked, id.toStr)
//  }
//
//  **
//  ** Read a list of records by id.  The resulting list matches
//  ** the list of ids by index (null if record not found).
//  **
//  Dict?[] readByIds(Ref[] ids, Bool checked := true)
//  {
//    if (ids.isEmpty)
//      return Dict[,]
//
//    refMap := Ref:Int[:]
//    res := Dict?[,].fill(null, ids.size)
//
//    // create query
//    sql := StrBuf()
//    sql.add("select brio from rec where id in (")
//    params := Str:Obj?[:]
//    ids.each |id, i|
//    {
//      refMap[id] = i
//
//      if (i > 0)
//        sql.add(", ")
//      sql.add("@x$i")
//      params.add("x$i", id.id)
//    }
//    sql.add(")")
//
//    // run query
//    stmt := conn.sql(sql.toStr).prepare
//    stmt.query(params).each |r|
//    {
//      d := BrioReader(((Buf)r->brio).in).readDict
//      res[refMap[d->id]] = d
//    }
//    stmt.close
//
//    if (checked && res.any |Dict? d->Bool| { d == null})
//      throw UnknownRecErr("missing ids")
//    else
//      return res
//  }
//
//  **
//  ** Return the number of records which match the given filter.
//  **
//  Int readCount(Filter filter, Dict? opts := null)
//  {
//    if (opts == null) opts = Etc.dict0
//    limit := opts.has("limit") ? opts->limit : Int.maxVal
//
//    q := Query.fromFilter(this, filter, true)
//
//    stmt := conn.sql(q.sql).prepare
//    rows := stmt.query(q.params)
//    Int res := rows[0]->count
//    stmt.close
//
//    return res < limit ? res : limit
//  }
//
//  **
//  ** Find the first record which matches the given filter.
//  ** Throw UnknownRecErr or return null based on checked flag.
//  **
//  Dict? read(Filter filter, Bool checked := true)
//  {
//    q := Query.fromFilter(this, filter)
//
//    stmt := conn.sql(q.sql).prepare
//    rows := stmt.query(q.params)
//    stmt.close
//
//    return doReadSingle(rows, checked, filter.toStr)
//  }
//
//  **
//  ** Match all the records against given filter.
//  **
//  Dict[] readAll(Filter filter, Dict? opts := null)
//  {
//    if (opts == null) opts = Etc.dict0
//    limit := opts.has("limit") ? opts->limit : Int.maxVal
//
//    q := Query.fromFilter(this, filter)
//
//    res := Dict[,]
//    stmt := conn.sql(q.sql).prepare
//    rows := stmt.query(q.params)
//    i := 0
//    while ((i < rows.size) && (i < limit))
//    {
//      res.add(BrioReader(((Buf)rows[i++]->brio).in).readDict)
//    }
//    stmt.close
//
//    return res
//  }
//
//  **
//  ** Read all records matching filter
//  **
//  Void readEach(Filter filter, Dict? opts, |Dict| func)
//  {
//    if (opts == null) opts = Etc.dict0
//    limit := opts.has("limit") ? opts->limit : Int.maxVal
//
//    q := Query.fromFilter(this, filter)
//
//    stmt := conn.sql(q.sql).prepare
//    try
//    {
//      i := 0
//      stmt.queryEach(q.params) |r|
//      {
//        if (i++ >= limit)
//          throw BreakErr()
//
//        func(BrioReader(((Buf)r->brio).in).readDict)
//      }
//    }
//    catch (BreakErr e) {}
//    stmt.close
//  }
//
//  **
//  ** Read all records matching filter until callback returns non-null
//  **
//  Obj? readEachWhile(Filter filter, Dict? opts, |Dict->Obj?| func)
//  {
//    if (opts == null) opts = Etc.dict0
//    limit := opts.has("limit") ? opts->limit : Int.maxVal
//
//    q := Query.fromFilter(this, filter)
//
//    Obj? res := null
//    stmt := conn.sql(q.sql).prepare
//    try
//    {
//      i := 0
//      stmt.queryEach(q.params) |r|
//      {
//        if (i++ >= limit)
//          throw BreakErr()
//
//        res = func(BrioReader(((Buf)r->brio).in).readDict)
//        if (res != null)
//          throw BreakErr()
//      }
//    }
//    catch (BreakErr e) {}
//    stmt.close
//
//    return res
//  }
//
//  **
//  ** Read a single value from a ResultSet
//  **
//  private Dict? doReadSingle(sql::Row[] rows, Bool checked, Str errMsg)
//  {
//    if (rows.isEmpty)
//    {
//      if (checked)
//        throw UnknownRecErr(errMsg)
//      else
//        return null
//    }
//    else
//    {
//      Buf brio := rows[0]->brio
//      return BrioReader(brio.in).readDict
//    }
//  }
//
////////////////////////////////////////////////////////////////////////////
//// Commits
////////////////////////////////////////////////////////////////////////////
//
//  **
//  ** Create new Spec record in the database
//  **
//  Void createSpec(Str qname, Str[] inheritsFrom)
//  {
//    inheritsFrom.each |from|
//    {
//      specInsert.execute([
//        "qname":qname,
//        "inheritsFrom": from
//      ])
//    }
//
//    conn.commit
//  }
//
//  **
//  ** Create new record in the database
//  **
//  Dict create(Dict tags, Ref? id := null)
//  {
//    // make sure there isn't already an id
//    if (tags.has("id"))
//      throw InvalidRecErr("tags cannot have 'id'")
//
//    // generate an id if need be
//    if (id == null)
//      id = Ref.gen
//
//    // add the id to the tags
//    tags = Etc.dictMerge(tags, Etc.dict1("id", id))
//
//    // insert main record
//    rec := Rec.fromDict(tags)
//    recInsert.execute([
//      "id":        rec.id,
//      "brio":      BrioWriter.valToBuf(tags),
//      "paths":     rec.paths,
//      "strs":      (rec.strs      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.strs),
//      "nums":      (rec.nums      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.nums),
//      "units":     (rec.units     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.units),
//      "bools":     (rec.bools     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.bools),
//      "uris":      (rec.uris      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.uris),
//      "dates":     (rec.dates     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dates),
//      "times":     (rec.times     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.times),
//      "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes),
//      "spec":      rec.spec
//    ])
//
//    // insert refs
//    insertPathRefs(rec.id, rec.refs)
//
//    conn.commit
//    return tags
//  }
//
//  ** Update existing record in the database.  If is mod is non-null, then
//  ** check mod timestamp to perform optimistic concurrency check.  If mod
//  ** is null, force update with no concurrency check.
//  Dict update(Ref id, Dict tags, DateTime? mod /* TODO ignored for now */)
//  {
//    rows := byIdSelect.query(["id": id.id])
//    Dict before := doReadSingle(rows, true, id.toStr)
//    beforeRefs := Rec.findRefPaths(before)
//
//    after := Etc.dictMerge(before, tags)
//
//    // update main record
//    rec := Rec.fromDict(after)
//    recUpdate.execute([
//      "id":        rec.id,
//      "brio":      BrioWriter.valToBuf(after),
//      "paths":     rec.paths,
//      "strs":      (rec.strs      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.strs),
//      "nums":      (rec.nums      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.nums),
//      "units":     (rec.units     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.units),
//      "bools":     (rec.bools     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.bools),
//      "uris":      (rec.uris      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.uris),
//      "dates":     (rec.dates     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dates),
//      "times":     (rec.times     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.times),
//      "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes),
//      "spec":      rec.spec
//    ])
//
//    // re-create refs if need be
//    if (beforeRefs != rec.refs)
//    {
//      pathRefDelete.execute(["source": id.id])
//      insertPathRefs(rec.id, rec.refs)
//    }
//
//    conn.commit
//    return after
//  }
//
//  ** Delete record from the database
//  Void delete(Ref id)
//  {
//    pathRefDelete.execute(["source": id.id])
//    recDelete.execute(["id": id.id])
//    conn.commit
//  }
//
//  ** Insert the path_ref records that go with a Rec
//  private Void insertPathRefs(Str id, Str:Str[] refs)
//  {
//    // insert refs
//    refs.each |targets, path|
//    {
//      // The last tag in a ref path always contains a Ref.
//
//      // Update ref tags if need be.
//      lt := lastTag(path)
//      if (!refTags.containsKey(lt))
//      {
//        refTagInsert.execute(["name": lt])
//        refTags[lt] = lt
//      }
//
//      // insert path refs
//      targets.each | target |
//      {
//        pathRefInsert.execute([
//          "source": id,
//          "path":   path,
//          "target": target
//        ])
//      }
//    }
//  }
//
////////////////////////////////////////////////////////////////////////////
//// Misc
////////////////////////////////////////////////////////////////////////////
//
//  ** Make a List of dotted Paths ending in Refs, using the refTag whitelist.
//  ** This will be used to construct a series of inner joins in
//  ** QueryBuilder.visitLeaf().
//  internal Str[] refPaths(FilterPath fp)
//  {
//    result := Str[,]
//
//    cur := Str[,]
//    for (i := 0; i < fp.size; i++)
//    {
//      tag := fp.get(i)
//      cur.add(tag)
//
//      if (refTags.containsKey(tag))
//      {
//        result.add(cur.join("."))
//        cur.clear
//      }
//    }
//
//    if (!cur.isEmpty)
//      result.add(cur.join("."))
//
//    return result
//  }
//
//  ** Find the last tag in a dotted path.
//  internal static Str lastTag(Str path)
//  {
//    n := path.indexr(".")
//    return (n == null) ? path : path[(n+1)..-1]
//  }
//
//  ** We use the connection internally in the test suite.
//  internal SqlConn? testConn() { conn }
//
////////////////////////////////////////////////////////////////////////////
//// Fields
////////////////////////////////////////////////////////////////////////////
//
//  private SqlConn? conn
//
//  private Statement? specInsert
//  private Statement? recInsert
//  private Statement? pathRefInsert
//  private Statement? refTagInsert
//  private Statement? byIdSelect
//  private Statement? recUpdate
//  private Statement? recDelete
//  private Statement? pathRefDelete
//
//  ** refTags is a Set that mirrors the records in the ref_tag table.
//  private Str:Str refTags := Str:Str[:]
//}
//
//**
//** BreakErr is used to break out of an 'each' loop
//**
//internal const class BreakErr : Err { new make() : super() {} }
