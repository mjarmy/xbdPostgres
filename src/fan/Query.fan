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
      ls.add("  inner join rec     r$i on r${i}.id     = p${i}.ref_")
    }
    ls.add("where")
    ls.add("${qb.where};")

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
    this.where = visit(f, 1)
  }

  internal Str visit(Filter f, Int indent)
  {
    if (f.type == FilterType.has)
      return visitLeaf(
        f.argA,
        null /*ignored*/,
        |Str[] paths, Obj? arg->Str| { hasParams(paths, arg) },
        indent)

    //else if (f.type == FilterType.eq)  visitEq(f.argA, f.argB)

    else if (f.type == FilterType.and)
      return visitAnd(f.argA, f.argB, indent)

    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  ** add the parameters for a 'has' Filter
  private Str hasParams(Str[] paths, Obj? arg /* ignored */)
  {
    alias := (paths.size == 1) ? "rec" : "r${joins}"
    n := params.size
    params.add("x$n", "{\"${paths[-1]}\"}")
    return "(${alias}.paths @> @x$n::text[])"
  }

  internal Str visitLeaf(
    FilterPath fp,
    Obj? arg,
    |Str[] paths, Obj? arg -> Str| paramFunc,
    Int indent)
  {
    pad := doIndent(indent)
    paths := dottedPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // add the parameter clause
      return pad + paramFunc(paths, arg)
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

      // add the parameter clause
      sb.add(paramFunc(paths, arg))

      sb.add(")")
      return pad + sb.toStr
    }
  }

  internal Str visitAnd(Filter a, Filter b, Int indent)
  {
    pad := doIndent(indent)

    return [
      pad + "(",
      visit(a, indent+1),
      pad + "  and",
      visit(b, indent+1),
      pad + ")",
    ].join("\n")
  }

  private static Str doIndent(Int indent)
  {
    sb := StrBuf()
    for (i := 0; i < indent; i++)
      sb.add("  ")
    return sb.toStr
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


