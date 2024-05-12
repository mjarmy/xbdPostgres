//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Query is a haystack Filter translated to parameterized SQL
**
const class Query
{
  new make(Str sql, Str:Obj params)
  {
    this.sql = sql
    this.params = params
  }

  **
  ** Create a Query from a Filter
  **
  static new fromFilter(Filter f)
  {
    qb := QueryBuilder(f)

    return Query(
      ["select * from rec",
       "where",
       "  ${qb.whereClause}"].join("\n"),
      qb.params)
  }

  override Int hash() { return sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Query
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Query:\n$sql\nparams:$params" }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  **
  ** The parameterized SQL
  **
  const Str sql

  **
  ** The parameter name-to-value Map.
  **
  const Str:Obj params
}

**
** QueryBuilder
**
internal class QueryBuilder {

  internal new make(Filter f)
  {
    visit(f)
  }

  internal Void visit(Filter f)
  {
    if      (f.type == FilterType.has) visitHas(f.argA)
    //else if (f.type == FilterType.eq)  visitEq(f.argA, f.argB)
    //else if (f.type == FilterType.and) visitAnd(f.argA, f.argB)
    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  internal Void visitHas(FilterPath fp)
  {
    paths := dottedPaths(fp)

    if (paths.size > 1)
    {
      throw UnsupportedErr("TODO")
    }

    n := whereClause.size
    params.add("p$n", "{\"${paths[0]}\"}")
    whereClause.add("(rec.paths @> @p$n::text[])")
  }

  ** make a List of dotted Paths, using BrioRefTags
  ** to define path boundaries
  internal static Str[] dottedPaths(FilterPath fp)
  {
    result := Str[,]

    cur := Str[,]
    for (i := 0; i < fp.size; i++)
    {
      tag := fp.get(i)
      cur.add(tag)

      // TODO is this really what we want?
      //if (BrioRefTags.cur.tags.containsKey(tag))
      if (tag.endsWith("Ref") || tag.endsWith("Of"))
      {
        result.add(cur.join("."))
        cur.clear
      }
    }

    if (!cur.isEmpty)
      result.add(cur.join("."))

    return result
  }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  internal StrBuf whereClause := StrBuf()
  internal Str:Obj params := Str:Obj[:]
}


