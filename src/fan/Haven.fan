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
  static Haven open(Str uri, Str? username, Str? password)
  {
    return Haven(uri, username, password)
  }

  private new make(Str uri, Str? username, Str? password)
  {
    this.conn = SqlConn.open(uri, username, password)
    conn.autoCommit = false

    this.specInsert = conn.sql(
      "insert into spec
         (qname, inherits_from)
       values
         (@qname, @inheritsFrom)").prepare

    this.recInsert = conn.sql(
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
     ).prepare

    this.pathRefInsert = conn.sql(
      "insert into path_ref
         (source, path_, target)
       values
         (@source, @path, @target)"
     ).prepare

    this.refTagInsert = conn.sql(
      "insert into ref_tag (name) values (@name)"
     ).prepare

    this.byIdSelect = conn.sql(
      "select brio from rec where id = @id"
    ).prepare;

    // load ref tags
    stmt := conn.sql("select name from ref_tag")
    stmt.query.each |r|
    {
      name := r->name
      refTags[name] = name
    }
    stmt.close
  }

  **
  ** Close the connection to Postgres
  **
  Void close()
  {
    specInsert.close
    recInsert.close
    pathRefInsert.close
    refTagInsert.close
    byIdSelect.close

    conn.close
    conn = null
  }

  **
  ** Insert a Spec
  **
  Void insertSpec(Str qname, Str[] inheritsFrom)
  {
    inheritsFrom.each |inhFrom|
    {
      specInsert.execute([
        "qname":qname,
        "inheritsFrom": inhFrom
      ])
    }

    conn.commit
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
      "dateTimes": (rec.dateTimes .isEmpty) ? null : JsonOutStream.writeJsonToStr(rec.dateTimes),
      "spec":      rec.spec
    ])

    rec.refs.each |targets, path|
    {
      // The last tag in a ref path always contains a Ref
      // Update ref tags if need be.
      lt := lastTag(path)
      if (!refTags.containsKey(lt))
      {
        refTagInsert.execute(["name": lt])
        refTags[lt] = lt
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

  ** Read single record by its id
  Dict? readById(Ref? id, Bool checked := true)
  {
    rows := byIdSelect.query(["id": id.id])

    if (rows.isEmpty)
    {
      if (checked)
        throw UnknownRecErr(id.toStr)
      else
        return null;
    }
    else
    {
      Buf brio := rows[0]->brio
      return BrioReader(brio.in).readDict
    }
  }

  **
  ** Select by list of ids
  **
  Dict[] selectByIds(Ref[] ids)
  {
    if (ids.isEmpty)
      return Dict[,]

    // create query
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
    sql.add(")")

    // run query
    res := Dict[,]
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

    stmt := conn.sql(q.sql).prepare
    stmt.query(q.params).each |r|
    {
      res.add(BrioReader(((Buf)r->brio).in).readDict)
    }
    stmt.close

    return res
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

  ** Find the last tag in a dotted path.
  internal static Str lastTag(Str path)
  {
    n := path.indexr(".")
    return (n == null) ? path : path[(n+1)..-1]
  }

  ** We use the connection internally in the test suite.
  internal SqlConn? testConn() { conn }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private SqlConn? conn

  private Statement? specInsert
  private Statement? recInsert
  private Statement? pathRefInsert
  private Statement? refTagInsert
  private Statement? byIdSelect

  ** refTags is a Set that mirrors the records in the ref_tag table.
  private Str:Str refTags := Str:Str[:]
}

