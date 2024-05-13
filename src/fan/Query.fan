//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Query translates a haystack Filter to parameterized SQL
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

    ls := ["select rec.* from rec"]
    for (i := 1; i <= qb.joins; i++)
    {
      prev := (i == 1) ? "rec" : "r${i-1}"
      ls.add("  inner join pathref p$i on p${i}.rec_id = ${prev}.id")
      ls.add("  inner join rec     r$i on r${i}.id     = p1.ref_")
    }
    ls.add("where")
    ls.add("  ${qb.where}")

    return Query(ls.join("\n"), qb.params)
  }

  override Int hash() { return sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Query
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Query:\n$sql\nparams:$params\n" }

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
    this.where = visit(f)
  }

  internal Str visit(Filter f)
  {
    if      (f.type == FilterType.has) return visitHas(f.argA)
    //else if (f.type == FilterType.eq)  visitEq(f.argA, f.argB)
    else if (f.type == FilterType.and) return visitAnd(f.argA, f.argB)
    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  internal Str visitHas(FilterPath fp)
  {
    paths := dottedPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // add the param where clause
      n := params.size
      params.add("x$n", "{\"${paths[0]}\"}")
      return "(rec.paths @> @x$n::text[])"
    }
    // joins
    else
    {
      sb := StrBuf()
      sb.add("(")

      // add the join where clauses
      last := paths.size-1
      for (i := 0; i < last; i++)
      {
        joins++
        n := params.size
        params.add("x$n", paths[i])
        sb.add("(p${joins}.path_ = @x$n) and ")
      }

      // add the param where clause
      n := params.size
      params.add("x$n", "{\"${paths[last]}\"}")
      sb.add("(r${joins}.paths @> @x$n::text[])")

      sb.add(")")
      return sb.toStr
    }
  }

  internal Str visitAnd(Filter a, Filter b)
  {
    return "(${visit(a)} and ${visit(b)})"
  }

  ** make a List of dotted Paths
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

  internal Str where
  internal Str:Obj params := Str:Obj[:]
  internal Int joins := 0
}


