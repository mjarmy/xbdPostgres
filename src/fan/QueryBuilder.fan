//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

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
        |Str alias, Str path, Obj? arg->Str| { hasParams(alias, path, arg) },
        indent)

    else if (f.type == FilterType.missing)
      return visitLeaf(
        f.argA,
        null /*ignored*/,
        |Str alias, Str path, Obj? arg->Str| { missingParams(alias, path, arg) },
        indent)

    else if (f.type == FilterType.eq)
      return visitLeaf(
        f.argA,
        f.argB,
        |Str alias, Str path, Obj? arg->Str| { eqParams(alias, path, arg) },
        indent)

//    else if (f.type == FilterType.ne)
//      return visitLeaf(
//        f.argA,
//        f.argB,
//        |Str alias, Str path, Obj? arg->Str| { neParams(alias, path, arg) },
//        indent)

    else if (f.type == FilterType.and)
      return visitAnd(f.argA, f.argB, indent)

    else if (f.type == FilterType.or)
      return visitOr(f.argA, f.argB, indent)

    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  private Str visitLeaf(
    FilterPath fp,
    Obj? arg,
    |Str alias, Str path, Obj? arg -> Str| paramFunc,
    Int indent)
  {
    pad := doIndent(indent)
    paths := dottedPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // add the parameter clause
      return pad + paramFunc("rec", paths[0], arg)
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
      sb.add(paramFunc("r${joins}", paths[-1], arg))

      sb.add(")")
      return pad + sb.toStr
    }
  }

  ** add the parameters for a 'has' Filter
  private Str hasParams(Str alias, Str path, Obj? arg /* ignored */)
  {
    n := params.size
    params.add("x$n", "{\"$path\"}")
    return "(${alias}.paths @> @x$n::text[])"
  }

  ** add the parameters for a 'missing' Filter
  private Str missingParams(Str alias, Str path, Obj? arg /* ignored */)
  {
    n := params.size
    params.add("x$n", "{\"$path\"}")
    return "(not (${alias}.paths @> @x$n::text[]))"
  }

  ** add the parameters for an 'eq' Filter
  private Str eqParams(Str alias, Str path, Obj? arg)
  {
return "TODO"
//    // convert DateTime to query format
//    if (arg is DateTime)
//      arg = Rec.convertDateTime(arg)
//
//    // Build up the containment dict by walking the keys backwards
//    Str[] keys := path.split('.')
//    dict := Etc.dict1(keys[-1], arg)
//    for (i := keys.size - 2; i >= 0; i--)
//      dict = Etc.dict1(keys[i], dict)
//
//    n := params.size
//    params.add("x$n", JsonWriter.valToStr(dict))
//    return "(${alias}.hayson @> @x$n::jsonb)"
  }

//  ** add the parameters for a 'ne' Filter
//  private Str neParams(Str alias, Str path, Obj? arg)
//  {
//    // convert DateTime to query format
//    if (arg is DateTime)
//      arg = Rec.convertDateTime(arg)
//
//    // Build up the containment dict by walking the keys backwards
//    Str[] keys := path.split('.')
//    dict := Etc.dict1(keys[-1], arg)
//    for (i := keys.size - 2; i >= 0; i--)
//      dict = Etc.dict1(keys[i], dict)
//
//    n := params.size
//    params.add("x$n", "{\"$path\"}")
//    hasClause := "(${alias}.paths @> @x$n::text[])"
//
//    n = params.size
//    params.add("x$n", JsonWriter.valToStr(dict))
//    eqClause := "(${alias}.hayson @> @x$n::jsonb)"
//
//    return "($hasClause and (not $eqClause))"
//  }

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


