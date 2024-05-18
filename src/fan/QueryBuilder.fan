//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using util

**
** QueryBuilder
**
internal class QueryBuilder {

  internal new make(Filter f)
  {
    this.where = visit(f, 1)
  }

  private Str visit(Filter f, Int indent)
  {
    if (f.type == FilterType.has)
      return visitLeaf(
        f.argA,
        null /*ignored*/,
        |Str alias, Str path, Obj? arg->Str| { has(alias, path, arg) },
        indent)

    else if (f.type == FilterType.missing)
      return visitLeaf(
        f.argA,
        null /*ignored*/,
        |Str alias, Str path, Obj? arg->Str| { missing(alias, path, arg) },
        indent)

    else if (f.type == FilterType.eq)
      return visitLeaf(
        f.argA,
        f.argB,
        |Str alias, Str path, Obj? arg->Str| { eq(alias, path, arg) },
        indent)

    else if (f.type == FilterType.ne)
      return visitLeaf(
        f.argA,
        f.argB,
        |Str alias, Str path, Obj? arg->Str| { ne(alias, path, arg) },
        indent)

    else if (f.type == FilterType.and)
      return visitAnd(f.argA, f.argB, indent)

    else if (f.type == FilterType.or)
      return visitOr(f.argA, f.argB, indent)

    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  private Str visitLeaf(
    FilterPath fp,
    Obj? arg,
    |Str alias, Str path, Obj? arg -> Str| nodeFunc,
    Int indent)
  {
    pad := doIndent(indent)
    paths := dottedPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // process the node
      return pad + nodeFunc("rec", paths[0], arg)
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

      // process the node
      sb.add(nodeFunc("r${joins}", paths[-1], arg))

      sb.add(")")
      return pad + sb.toStr
    }
  }

  ** 'has' AST node
  private Str has(Str alias, Str path, Obj? ignored := null)
  {
    n := params.size
    params.add("x$n", "{\"$path\"}")
    return "(${alias}.paths @> @x$n::text[])"
  }

  ** 'missing' AST node
  private Str missing(Str alias, Str path, Obj? ignored)
  {
    n := params.size
    params.add("x$n", "{\"$path\"}")
    return "(not (${alias}.paths @> @x$n::text[]))"
  }

  ** 'eq' AST node
  private Str eq(Str alias, Str path, Obj? val)
  {
    // Ref
    if (val is Ref)
    {
      param := addValParam(path, ((Ref) val).id)
      return "(${alias}.refs @> @$param::jsonb)"
    }
    // Str
    else if (val is Str)
    {
      param := addValParam(path, (Str) val)
      return "(${alias}.strs @> @$param::jsonb)"
    }

    // val type cannot be used for this node
    else
      return "false";
  }

  ** add the parameters for a 'ne' Filter
  private Str ne(Str alias, Str path, Obj? arg)
  {
    hasClause := has(alias, path)
    eqClause := eq(alias, path, arg)
    return "($hasClause and (not $eqClause))"
  }

  private Str addValParam(Str path, Obj? val)
  {
    name := "x${params.size}"
    params.add(
      name,
      JsonOutStream.writeJsonToStr(
        Str:Obj[path:val]))
    return name
  }

  private Str visitAnd(Filter a, Filter b, Int indent)
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

  private Str visitOr(Filter a, Filter b, Int indent)
  {
    pad := doIndent(indent)

    return [
      pad + "(",
      visit(a, indent+1),
      pad + "  or",
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

      // TODO whitelist
      if ((tag == "id") || tag.endsWith("Ref") || tag.endsWith("Of"))
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

  internal Str where
  internal Str:Obj params := Str:Obj[:]
  internal Int joins := 0
}


