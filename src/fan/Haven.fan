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
const class Haven
{
  new make(|This|? f) {
    if (f != null) f(this)

    useSchema = "set search_path to $projName"
  }

  Void init()
  {
    pool.execute(|SqlConn conn|
    {
      if (!schemaExists(conn, projName))
        createSchema(conn)

      loadRefTags(conn)

      conn.commit
    })
  }

  internal static Bool schemaExists(SqlConn conn, Str projName)
  {
    stmt := conn.sql(
      "select exists (
        select schema_name from information_schema.schemata
        where schema_name = @projName)"
      ).prepare

    Bool exists := stmt.query([
      "projName": projName
      ]).first->exists

    stmt.close
    return exists
  }

// This works, we just don't need it right now
//  private Bool tableExists(SqlConn conn, Str tableName)
//  {
//    stmt := conn.sql(
//      "select exists (
//          select from pg_tables
//          where  schemaname = @schemaName
//          and    tablename  = @tableName)"
//      ).prepare
//
//    Bool exists := stmt.query([
//      "schemaName": projName,
//      "tableName":  tableName
//      ]).first->exists
//
//    stmt.close
//    return exists
//  }

  private Void createSchema(SqlConn conn)
  {
    conn.sql(
      "create schema $projName;
       set search_path to $projName;

       -- Specs
       create table spec (
         qname         text not null,
         inherits_from text not null,
         constraint spec_pkey primary key (qname, inherits_from)
       );
       create index spec_inherits_from on spec (inherits_from);

       -- Ref_tag is the set of every tag that contains a ref
       create table ref_tag (
         name text primary key
       );

       -- Recs
       create table rec (
         id text   primary key,
         brio      bytea  not null,
         paths     text[] not null,
         strs      jsonb,
         nums      jsonb,
         units     jsonb,
         bools     jsonb,
         uris      jsonb,
         dates     jsonb,
         times     jsonb,
         dateTimes jsonb,
         spec       text -- implicit foreign key to spec(qname)
       );
       create index rec_paths     on rec using gin (paths);
       create index rec_strs      on rec using gin (strs      jsonb_path_ops);
       create index rec_nums      on rec using gin (nums      jsonb_path_ops);
       create index rec_units     on rec using gin (units     jsonb_path_ops);
       create index rec_bools     on rec using gin (bools     jsonb_path_ops);
       create index rec_uris      on rec using gin (uris      jsonb_path_ops);
       create index rec_dates     on rec using gin (dates     jsonb_path_ops);
       create index rec_times     on rec using gin (times     jsonb_path_ops);
       create index rec_dateTimes on rec using gin (dateTimes jsonb_path_ops);
       create index rec_spec      on rec (spec);

       -- Ref lookups via self-join
       create table path_ref (
         source text not null references rec (id),
         path_  text not null,
         target text not null, -- implicit foreign key to rec(id)
         constraint path_ref_pkey primary key (source, path_, target)
       );
       create index path_ref_path_target on path_ref (path_, target);"
     ).execute
  }

  private Void loadRefTags(SqlConn conn)
  {
    conn.sql(useSchema).execute
    stmt := conn.sql("select name from ref_tag")
    stmt.query.each |r|
    {
      name := r->name
      refTags[name] = name
    }
    stmt.close
  }

//////////////////////////////////////////////////////////////////////////
// Reads
//////////////////////////////////////////////////////////////////////////

  **
  ** Read single record by its id
  **
  Dict? readById(Ref? id, Bool checked := true)
  {
    Buf? result := null

    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, selectById)
      rows := stmt.query(["id": id.id])
      result = doReadSingle(rows, checked, id.toStr)
    })

    return (result == null) ? null : BrioReader(result.in).readDict
  }

  **
  ** Read a list of records by id.  The resulting list matches
  ** the list of ids by index (null if record not found).
  **
  Dict?[] readByIds(Ref[] ids, Bool checked := true)
  {
    if (ids.isEmpty)
      return Dict[,]

    // create SQL
    sb := StrBuf(64)
    sb.add("select brio from rec where id in (")
    ids.each |id, i|
    {
      if (i > 0)
        sb.add(", ")
      sb.add("@x$i")
    }
    sb.add(")")
    sql := sb.toStr

    // params
    params := Str:Obj?[:]
    refMap := Ref:Int[:]
    ids.each |id, i|
    {
      refMap[id] = i
      params.add("x$i", id.id)
    }

    // run query
    bufs := Buf[,]
    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, sql)
      stmt.query(params).each |r|
      {
        bufs.add(r->brio)
      }
    })

    // convert to dicts
    res := Dict?[,].fill(null, ids.size)
    bufs.each |b|
    {
      d := BrioReader(b.in).readDict
      res[refMap[d->id]] = d
    }

    if (checked && res.any |Dict? d->Bool| { d == null })
      throw UnknownRecErr("missing ids")
    else
      return res
  }

  **
  ** Return the number of records which match the given filter.
  **
  Int readCount(Filter filter, Dict? opts := null)
  {
    if (opts == null) opts = Etc.dict0
    limit := opts.has("limit") ? opts->limit : Int.maxVal
    res := 0

    q := Query.fromFilter(this, filter, true)

    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, q.sql)
      rows := stmt.query(q.params)
      res = rows[0]->count
    })

    return res < limit ? res : limit
  }

  **
  ** Find the first record which matches the given filter.
  ** Throw UnknownRecErr or return null based on checked flag.
  **
  Dict? read(Filter filter, Bool checked := true)
  {
    Buf? result := null

    q := Query.fromFilter(this, filter)

    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, q.sql)
      rows := stmt.query(q.params)
      result = doReadSingle(rows, checked, filter.toStr)
    })

    return (result == null) ? null : BrioReader(result.in).readDict
  }

  **
  ** Match all the records against given filter.
  **
  Dict[] readAll(Filter filter, Dict? opts := null)
  {
    if (opts == null) opts = Etc.dict0
    limit := opts.has("limit") ? opts->limit : Int.maxVal

    q := Query.fromFilter(this, filter)

    res := Buf[,]
    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, q.sql)
      rows := stmt.query(q.params)
      i := 0
      while ((i < rows.size) && (i < limit))
      {
        res.add(rows[i++]->brio)
      }
    })

    return res.map(|r->Dict| { BrioReader(r.in).readDict } )
  }

  **
  ** Read all records matching filter
  **
  Void readEach(Filter filter, Dict? opts, |Dict| func)
  {
    if (opts == null) opts = Etc.dict0
    limit := opts.has("limit") ? opts->limit : Int.maxVal

    q := Query.fromFilter(this, filter)

    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, q.sql)
      i := 0
      stmt.queryEachWhile(q.params) |r|
      {
        if (i++ >= limit)
          return LimitReached.val

        func(BrioReader(((Buf)r->brio).in).readDict)
        return null
      }
    })
  }

  **
  ** Read all records matching filter until callback returns non-null
  **
  Obj? readEachWhile(Filter filter, Dict? opts, |Dict->Obj?| func)
  {
    if (opts == null) opts = Etc.dict0
    limit := opts.has("limit") ? opts->limit : Int.maxVal

    q := Query.fromFilter(this, filter)

    Obj? res := null
    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, q.sql)
      i := 0
      stmt.queryEachWhile(q.params) |r|
      {
        if (i++ >= limit)
          return LimitReached.val

        res = func(BrioReader(((Buf)r->brio).in).readDict)
        return res
      }
    })

    return (res === LimitReached.val) ? null : res
  }

  **
  ** Read a single value from a ResultSet
  **
  private Buf? doReadSingle(sql::Row[] rows, Bool checked, Str errMsg)
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
      return rows[0]->brio
    }
  }

//////////////////////////////////////////////////////////////////////////
// QueryBuilder
//////////////////////////////////////////////////////////////////////////

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
// Commits
//////////////////////////////////////////////////////////////////////////

  **
  ** Create new record in the database
  **
  Dict create(Dict tags, Ref? id := null)
  {
    // add an id to the tags
    if (tags.has("id"))
      throw Err("tags cannot have 'id'")
    tags = Etc.dictMerge(tags, Etc.dict1("id", id ?: Ref.gen))

    // build params
    rec := Rec.fromDict(tags)
    params := Str:Obj?[
      "id":        rec.id,
      "brio":      BrioWriter.valToBuf(tags),
      "paths":     rec.paths,
      "strs":      (rec.strs      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.strs),
      "nums":      (rec.nums      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.nums),
      "units":     (rec.units     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.units),
      "bools":     (rec.bools     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.bools),
      "uris":      (rec.uris      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.uris),
      "dates":     (rec.dates     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dates),
      "times":     (rec.times     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.times),
      "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes),
      "spec":      rec.spec
    ]

    doExecute(|SqlConn conn|
    {
      fetch(conn, insertRec).execute(params)
      insertPathRefs(conn, rec.id, rec.refs)
      conn.commit
    })

    return tags
  }

  ** Update existing record in the database.  If is mod is non-null, then
  ** check mod timestamp to perform optimistic concurrency check.  If mod
  ** is null, force update with no concurrency check.
  Dict update(Ref id, Dict tags, DateTime? mod /* TODO ignored for now */)
  {
    Dict? after := null

    doExecute(|SqlConn conn|
    {
      // fetch the current row
      rows := fetch(conn, selectById).query(["id": id.id])
      Buf? brio := doReadSingle(rows, true, id.toStr)
      Dict before := BrioReader(brio.in).readDict
      beforeRefs := Rec.findRefPaths(before)

      // construct the new row
      after = Etc.dictMerge(before, tags)

      // Build params.  It would be nice for efficiency's sake if we could do
      // this outside of the execute() closure, but sadly we cannot, since we
      // have to read the 'before' row as part of this transaction.
      rec := Rec.fromDict(after)
      params := Str:Obj?[
        "id":        rec.id,
        "brio":      BrioWriter.valToBuf(after),
        "paths":     rec.paths,
        "strs":      (rec.strs      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.strs),
        "nums":      (rec.nums      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.nums),
        "units":     (rec.units     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.units),
        "bools":     (rec.bools     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.bools),
        "uris":      (rec.uris      .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.uris),
        "dates":     (rec.dates     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dates),
        "times":     (rec.times     .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.times),
        "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes),
        "spec":      rec.spec
      ]

      // update main record
      fetch(conn, updateRec).execute(params)

      // re-create refs if need be
      if (beforeRefs != rec.refs)
      {
        fetch(conn, deletePathRef).execute(["source": id.id])
        insertPathRefs(conn, rec.id, rec.refs)
      }

      conn.commit
    })

    return after
  }

  ** Delete record from the database
  Void delete(Ref id)
  {
    doExecute(|SqlConn conn|
    {
      fetch(conn, deletePathRef).execute(["source": id.id])
      fetch(conn, deleteRec).execute(["id": id.id])
      conn.commit
    })
  }

  ** Insert the path_ref records that go with a Rec.
  ** Also insert any needed ref_tag records.
  private Void insertPathRefs(SqlConn conn, Str id, Str:Str[] refs)
  {
    pathRefInsert := fetch(conn, insertPathRef)
    refTagInsert := fetch(conn, insertRefTag)

    // insert refs
    refs.each |targets, path|
    {
      // Update ref tags if need be.
      lt := Rec.lastTag(path)
      if (!refTags.containsKey(lt))
      {
        refTagInsert.execute(["name": lt])
        refTags[lt] = lt
      }

      // insert path refs
      targets.each | target |
      {
        pathRefInsert.execute([
          "source": id,
          "path":   path,
          "target": target
        ])
      }
    }
  }

  **
  ** Create new Spec record in the database
  **
  Void createSpec(Str qname, Str[] inheritsFrom)
  {
    doExecute(|SqlConn conn|
    {
      stmt := fetch(conn, insertSpec)

      inheritsFrom.each |from|
      {
        stmt.execute([
          "qname":qname,
          "inheritsFrom": from
        ])
      }

      conn.commit
    })
  }

//////////////////////////////////////////////////////////////////////////
// Private Statements
//////////////////////////////////////////////////////////////////////////

  ** Fetch a prepared statement for this project from the connection's stash,
  ** or create the prepared statement.
  private Statement fetch(SqlConn conn, Str sql)
  {
    Str:Statement smap := conn.stash.getOrAdd(
      projName, |Str k->Obj?| { return Str:Statement[:] })

    return smap.getOrAdd(sql, |Str k->Statement|
    {
      echo("Haven.fetch: $conn $projName '$sql'")
      return conn.sql(sql).prepare
    })
  }

  ** Execute some statments against the current schema.
  private Void doExecute(|SqlConn conn| func)
  {
    pool.execute(|SqlConn conn|
    {
      conn.sql(useSchema).execute
      func(conn)
    })
  }

//////////////////////////////////////////////////////////////////////////
// SQL constants
//////////////////////////////////////////////////////////////////////////

  private static const Str selectById :=
    "select brio from rec where id = @id"

  private static const Str insertRec :=
    "insert into rec (
       id, brio, paths,
       strs, nums, units,
       bools, uris,
       dates, times, dateTimes,
       spec)
     values (
       @id, @brio, @paths,
       @strs::jsonb, @nums::jsonb, @units::jsonb,
       @bools::jsonb, @uris::jsonb,
       @dates::jsonb, @times::jsonb, @dateTimes::jsonb,
       @spec)"

  private static const Str insertPathRef :=
    "insert into path_ref
       (source, path_, target)
     values
       (@source, @path, @target)"

  private static const Str insertRefTag :=
    "insert into ref_tag (name) values (@name)"

  private static const Str updateRec :=
    "update rec set
       brio      = @brio,
       paths     = @paths,
       strs      = @strs::jsonb,
       nums      = @nums::jsonb,
       units     = @units::jsonb,
       bools     = @bools ::jsonb,
       uris      = @uris::jsonb,
       dates     = @dates ::jsonb,
       times     = @times ::jsonb,
       dateTimes = @dateTimes::jsonb,
       spec      = @spec
     where id = @id"

  private static const Str deleteRec :=
    "delete from rec where id = @id"

  private static const Str deletePathRef :=
    "delete from path_ref where source = @source"

  private static const Str insertSpec :=
    "insert into spec
       (qname, inherits_from)
     values
       (@qname, @inheritsFrom)"

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  public const Str projName
  internal const HavenPool pool

  private const Str useSchema

  ** refTags is a Set that mirrors the records in the ref_tag table.
  private const ConcurrentMap refTags := ConcurrentMap()
}

**************************************************************************
** LimitReached is a singleton that represents the fact that we have
** reached the limit inside of a queryEachWhile()
**************************************************************************

internal const final class LimitReached
{
  ** Singleton value
  const static LimitReached val := LimitReached()

  private new make() {}
}
