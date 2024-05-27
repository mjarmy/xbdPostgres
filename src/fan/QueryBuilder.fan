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

  ** construct from filter
  internal new make(Haven haven, Filter f)
  {
    this.haven = haven
    this.where = visit(f, 1)
  }

  ** create the query
  internal Query toQuery()
  {
    sql := StrBuf()

    if (joins > 0)
    {
      // We have to order the records if we are doing a join, so we include
      // the id in the ResultSet.
      //
      // This is necessary because list-of-refs will cause duplicate records to
      // be returned. We remove them in Haven by comparing ids as we process
      // each Row.
      sql.add("select rec.id, rec.brio from rec\n")

      for (i := 1; i <= joins; i++)
      {
        prev := (i == 1) ? "rec" : "r${i-1}"
        sql.add("  inner join path_ref p$i on p${i}.source = ${prev}.id\n")
        sql.add("  inner join rec      r$i on r${i}.id     = p${i}.target\n")
      }

      sql.add("where\n")
      sql.add(where).add("\n")

      // we have to order the records if we are doing a join
      sql.add("order by rec.id")
    }
    else
    {
      sql.add("select rec.brio from rec\n")
      sql.add("where\n")
      sql.add(where)
    }

    return Query(sql.toStr, params)
  }

  ** visit AST node
  private Str visit(Filter f, Int indent)
  {
    switch (f.type)
    {
    case FilterType.and:
      return visitAnd(f.argA, f.argB, indent)

    case FilterType.or:
      return visitOr(f.argA, f.argB, indent)

    case FilterType.isSpec:
      return visitIsSpec((Str)f.argA, indent)

    case FilterType.has:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { has(alias, path) },
        indent)

    case FilterType.missing:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { missing(alias, path) },
        indent)

    case FilterType.eq:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { eq(alias, path, f.argB) },
        indent)

    case FilterType.ne:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { ne(alias, path, f.argB) },
        indent)

    case FilterType.lt:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, "<") },
        indent)

    case FilterType.le:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, "<=") },
        indent)

    case FilterType.gt:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, ">") },
        indent)

    case FilterType.ge:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, ">=") },
        indent)

    default:
        throw Err("Encountered unknown FilterType ${f.type}")
    }
  }

  ** visit 'and'
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

  ** visit 'or'
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

  ** visit 'isSpec'
  private Str visitIsSpec(Str spec, Int indent)
  {
    specs++
    x := addParam(spec)

    // use a nested subquery
    return [
      doIndent(indent),
      "(exists (select 1 from spec s$specs where s${specs}.qname = rec.spec ",
      "and s${specs}.inherits_from = @$x))"
    ].join()
  }

  ** visit a leaf AST node
  private Str visitLeaf(
    FilterPath fp,
    |Str alias, Str path-> Str| nodeFunc,
    Int indent)
  {
    pad := doIndent(indent)

    // Create the paths using Haven's whitelisted ref paths
    paths := haven.refPaths(fp)

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
    // Misc toStr()
    if ((val is Str) || (val is Uri) || (val is Date) || (val is Time))
    {
      x := addJsonParam(path, val.toStr)
      col := columnNames[val.typeof]
      return "(${alias}.$col @> @$x::jsonb)"
    }
    // Ref
    else if (val is Ref)
    {
      return refEq(alias, path, val)
    }
    // Num
    else if (val is Number)
    {
      Number n := (Number) val
      xn := addJsonParam(path, n.toFloat)
      xu := addJsonParam(path, n.unit == null ? null : n.unit.toStr)
      return "((${alias}.nums @> @$xn::jsonb) and (${alias}.units @> @$xu::jsonb))"
    }
    // Bool
    else if (val is Bool)
    {
      x := addJsonParam(path, val)
      return "(${alias}.bools @> @$x::jsonb)"
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val
      xn := addJsonParam(path, Duration(ts.ticks).toMillis)
      return "(${alias}.dateTimes @> @$xn::jsonb)"
    }

    // val type cannot be used for this node
    else
      return "false";
  }

  ** 'ne' AST node
  private Str ne(Str alias, Str path, Obj? val)
  {
    // Ref
    if (val is Ref)
    {
      return "(not " + refEq(alias, path, val) + ")"
    }
    // anything else
    else
    {
      hasClause := has(alias, path)
      eqClause := eq(alias, path, val)
      col := columnNames[val.typeof]

      // We have to check if the column is null because of 3-Value booleans
      return "($hasClause and ((${alias}.$col is null) or (not $eqClause)))"
    }
  }

  ** test a ref for equality using a nested subquery
  private Str refEq(Str alias, Str path, Ref ref)
  {
    valRefs++
    xp := addParam(path)
    xv := addParam(ref.id)
    return [
      "(exists (select 1 from path_ref v$valRefs ",
      "where v${valRefs}.source = ${alias}.id ",
      "and v${valRefs}.path_ = @$xp ",
      "and v${valRefs}.target = @$xv))"
    ].join()
  }

  ** 'cmp' AST node >,>=,<,<=
  private Str cmp(Str alias, Str path, Obj? val, Str op)
  {
    // https://hashrocket.com/blog/posts/dealing-with-nested-json-objects-in-postgresql
    // https://stackoverflow.com/questions/53841916/how-to-compare-numeric-in-postgresql-jsonb

    // Misc toStr()
    if ((val is Str) || (val is Date) || (val is Time))
    {
      hasClause := has(alias, path)
      col := columnNames[val.typeof]

      // double-stabby gives us '::text'
      xp := addParam(path)
      xv := addParam(val.toStr)
      cmpClause := "((${alias}.$col ->> @$xp) $op @$xv)";

      return "($hasClause and $cmpClause)"
    }
    // Number
    else if (val is Number)
    {
      Number n := (Number) val

      hasClause := has(alias, path)

      // single-stabby plus cast
      xp := addParam(path)
      xv := addParam(n.toFloat)
      cmpClause := "(((${alias}.nums -> @$xp)::real) $op @$xv)";

      xu := addJsonParam(path, n.unit == null ? null : n.unit.toStr)
      unitEqClause := "(${alias}.units @> @$xu::jsonb)"

      return "($hasClause and $cmpClause and $unitEqClause)"
    }
    // Bool
    else if (val is Bool)
    {
      hasClause := has(alias, path)

      // single-stabby plus cast
      xp := addParam(path)
      xv := addParam(val)
      cmpClause := "(((${alias}.bools -> @$xp)::boolean) $op @$xv)";

      return "($hasClause and $cmpClause)"
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val

      hasClause := has(alias, path)

      // single-stabby plus cast
      xp := addParam(path)
      xv := addParam(Duration(ts.ticks).toMillis)
      cmpClause := "(((${alias}.dateTimes -> @$xp)::bigint) $op @$xv)";

      return "($hasClause and $cmpClause)"
    }

    // val type cannot be used for this node
    else
      return "false";
  }

  ** add a JSON param for a path:val pair
  private Str addJsonParam(Str path, Obj? val)
  {
    return addParam(
      JsonOutStream.writeJsonToStr(
        Str:Obj?[path:val]))
  }

  ** add a param
  private Str addParam(Obj val)
  {
    name := "x${params.size}"
    params.add(name, val)
    return name
  }

  ** create some indentation padding
  private static Str doIndent(Int indent)
  {
    sb := StrBuf()
    for (i := 0; i < indent; i++)
      sb.add("  ")
    return sb.toStr
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private static const Type:Str columnNames := Type:Str[
    Uri#:      "uris",
    Str#:      "strs",
    Number#:   "nums",
    Bool#:     "bools",
    Date#:     "dates",
    Time#:     "times",
    DateTime#: "dateTimes",
  ]

  private Haven haven

  private Str where
  private Str:Obj params := Str:Obj[:]
  private Int joins := 0

  private Int specs := 0
  private Int valRefs := 0
}


