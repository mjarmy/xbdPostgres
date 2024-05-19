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
    if (f.type == FilterType.and)
      return visitAnd(f.argA, f.argB, indent)

    else if (f.type == FilterType.or)
      return visitOr(f.argA, f.argB, indent)

    else if (f.type == FilterType.has)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { has(alias, path) },
        indent)

    else if (f.type == FilterType.missing)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { missing(alias, path) },
        indent)

    else if (f.type == FilterType.eq)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { eq(alias, path, f.argB) },
        indent)

    else if (f.type == FilterType.ne)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { ne(alias, path, f.argB) },
        indent)

    else if (f.type == FilterType.lt)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, "<") },
        indent)

    else if (f.type == FilterType.le)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, "<=") },
        indent)

    else if (f.type == FilterType.gt)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, ">") },
        indent)

    else if (f.type == FilterType.ge)
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, ">=") },
        indent)

    else throw Err("Encountered unknown FilterType ${f.type}")
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

  private Str visitLeaf(
    FilterPath fp,
    |Str alias, Str path-> Str| nodeFunc,
    Int indent)
  {
    pad := doIndent(indent)
    paths := dottedPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // process the node
      return pad + nodeFunc("rec", paths[0])
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
        x := addParam(paths[i])
        sb.add("(p${joins}.path_ = @$x) and ")
      }

      // process the node
      sb.add(nodeFunc("r${joins}", paths[-1]))

      sb.add(")")
      return pad + sb.toStr
    }
  }

  ** 'has' AST node
  private Str has(Str alias, Str path)
  {
    x := addParam("{\"$path\"}")
    return "(${alias}.paths @> @$x::text[])"
  }

  ** 'missing' AST node
  private Str missing(Str alias, Str path)
  {
    name := addParam("{\"$path\"}")
    return "(not (${alias}.paths @> @$name::text[]))"
  }

  ** 'eq' AST node
  private Str eq(Str alias, Str path, Obj? val)
  {
    // Ref
    if (val is Ref)
    {
      x := eqParam(path, ((Ref) val).id)
      return "(${alias}.refs @> @$x::jsonb)"
    }
    // Str
    else if (val is Str)
    {
      x := eqParam(path, val)
      return "(${alias}.strs @> @$x::jsonb)"
    }

    // val type cannot be used for this node
    else
      return "false";
  }

  private Str eqParam(Str path, Obj? val)
  {
    return addParam(
      JsonOutStream.writeJsonToStr(
        Str:Obj[path:val]))
  }

  ** add the parameters for a 'ne' Filter
  private Str ne(Str alias, Str path, Obj? val)
  {
    hasClause := has(alias, path)
    eqClause := eq(alias, path, val)
    return "($hasClause and (not $eqClause))"
  }

  ** 'cmp' AST node -- >,>=,<,<=
  private Str cmp(Str alias, Str path, Obj? val, Str op)
  {
    // Str
    if (val is Str)
    {
      hasClause := has(alias, path)

      xp := addParam(path)
      xv := addParam(val)
      cmpClause := "((${alias}.strs->>@$xp)::text $op @$xv)";

      return "($hasClause and $cmpClause)"
    }

    // val type cannot be used for this node
    else
      return "false";
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

  internal Str addParam(Obj val)
  {
    name := "x${params.size}"
    params.add(name, val)
    return name
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal Str where
  internal Str:Obj params := Str:Obj[:]
  internal Int joins := 0
}


