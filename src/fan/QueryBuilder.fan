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
  internal new make(Haven haven, Filter f, Bool isCount)
  {
    this.haven = haven

    if (isCount)
      sql.add("select count(*) as count from rec where ")
    else
      sql.add("select rec.brio from rec where ")

    sql.add(visit(f))
  }

  ** create the query
  internal Query toQuery()
  {
    return Query(sql.toStr, params)
  }

  ** visit AST node
  private Str visit(Filter f)
  {
    switch (f.type)
    {
    case FilterType.and:
      return visitAnd(f.argA, f.argB)

    case FilterType.or:
      return visitOr(f.argA, f.argB)

    case FilterType.isSpec:
      return visitIsSpec((Str)f.argA)

    case FilterType.has:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { has(alias, path) })

    case FilterType.missing:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { missing(alias, path) })

    case FilterType.eq:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { eq(alias, path, f.argB) })

    case FilterType.ne:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { ne(alias, path, f.argB) })

    case FilterType.lt:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, "<") })

    case FilterType.le:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, "<=") })

    case FilterType.gt:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, ">") })

    case FilterType.ge:
      return visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path->Str| { cmp(alias, path, f.argB, ">=") })

    default:
        throw Err("Encountered unknown FilterType ${f.type}")
    }
  }

  ** visit 'and'
  private Str visitAnd(Filter a, Filter b)
  {
    sb := StrBuf()
    sb.add("(")
    sb.add(visit(a))
    sb.add(" and ")
    sb.add(visit(b))
    sb.add(")")
    return sb.toStr
  }

  ** visit 'or'
  private Str visitOr(Filter a, Filter b)
  {
    sb := StrBuf()
    sb.add("(")
    sb.add(visit(a))
    sb.add(" or ")
    sb.add(visit(b))
    sb.add(")")
    return sb.toStr
  }

  ** visit 'isSpec'
  private Str visitIsSpec(Str spec)
  {
    specs++
    x := addParam(spec)

    // use a nested subquery
    sb := StrBuf()
    sb.add("(exists (select 1 from spec s$specs where s${specs}.qname = rec.spec ")
    sb.add("and s${specs}.inherits_from = @$x))")
    return sb.toStr
  }

  ** visit a leaf AST node
  private Str visitLeaf(
    FilterPath fp,
    |Str alias, Str path-> Str| nodeFunc)
  {
    // Create the paths using Haven's whitelisted ref paths
    paths := haven.refPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // process the node
      return nodeFunc("rec", paths[0])
    }
    // joins
    else
    {
      a := joins + 1
      joins += paths.size-1

      sb := StrBuf()
      sb.add("(exists (")
      sb.add("select 1 from path_ref p${a} ")
      sb.add("inner join rec r${a} on r${a}.id = p${a}.target ")

      for (i := a+1; i <= joins; i++)
      {
        sb.add("inner join path_ref p$i on p${i}.source = r${i-1}.id ")
        sb.add("inner join rec r$i on r${i}.id = p${i}.target ")
      }

      sb.add("where (p${a}.source = rec.id) and ")
      for (i := 0; i < paths.size-1; i++)
      {
        x := addParam(paths[i])
        sb.add("(p${a+i}.path_ = @$x) and ")
      }
      // process the node
      sb.add(nodeFunc("r${joins}", paths[-1]))

      sb.add("))")

      return sb.toStr
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
      x := addObjParam(path, val.toStr)
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
      xn := addObjParam(path, n.toFloat)
      xu := addObjParam(path, n.unit == null ? null : n.unit.toStr)
      return "((${alias}.nums @> @$xn::jsonb) and (${alias}.units @> @$xu::jsonb))"
    }
    // Bool
    else if (val is Bool)
    {
      x := addObjParam(path, val)
      return "(${alias}.bools @> @$x::jsonb)"
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val
      xn := addObjParam(path, Duration(ts.ticks).toMillis)
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

    sb := StrBuf()
    sb.add("(exists (select 1 from path_ref v$valRefs ")
    sb.add("where v${valRefs}.source = ${alias}.id ")
    sb.add("and v${valRefs}.path_ = @$xp ")
    sb.add("and v${valRefs}.target = @$xv))")
    return sb.toStr
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

      xu := addObjParam(path, n.unit == null ? null : n.unit.toStr)
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

  ** add a JSON Object param for a path:val pair
  private Str addObjParam(Str path, Obj? val)
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

  private StrBuf sql := StrBuf()
  private Str:Obj params := Str:Obj[:]

  private Int joins := 0
  private Int specs := 0
  private Int valRefs := 0
}


