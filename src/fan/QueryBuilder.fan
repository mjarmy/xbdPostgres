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
      sql.add("select count(*) as count from rec r0 where ")
    else
      sql.add("select brio from rec r0 where ")

    visit(f)
  }

  ** create the query
  internal Query toQuery()
  {
    map := Str:Obj[:]
    b := StrBuf()
    params.each |v, i|
    {
      b.add("x").add(i)
      map[b.toStr] = v
      b.clear
    }

    return Query(sql.toStr, map)
  }

  ** visit AST node
  private Void visit(Filter f)
  {
    switch (f.type)
    {
    case FilterType.and:
      visitAnd(f.argA, f.argB)

    case FilterType.or:
      visitOr(f.argA, f.argB)

    case FilterType.isSpec:
      visitIsSpec((Str)f.argA)

    case FilterType.has:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { has(alias, path) })

    case FilterType.missing:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { missing(alias, path) })

    case FilterType.eq:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { eq(alias, path, f.argB) })

    case FilterType.ne:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { ne(alias, path, f.argB) })

    case FilterType.lt:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { cmp(alias, path, f.argB, "<") })

    case FilterType.le:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { cmp(alias, path, f.argB, "<=") })

    case FilterType.gt:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { cmp(alias, path, f.argB, ">") })

    case FilterType.ge:
      visitLeaf(
        (FilterPath)f.argA,
        |Int alias, Str path| { cmp(alias, path, f.argB, ">=") })

    default:
        throw Err("Encountered unknown FilterType ${f.type}")
    }
  }

  ** visit 'and'
  private Void visitAnd(Filter a, Filter b)
  {
    sql.add("(")
    visit(a)
    sql.add(" and ")
    visit(b)
    sql.add(")")
  }

  ** visit 'or'
  private Void visitOr(Filter a, Filter b)
  {
    sql.add("(")
    visit(a)
    sql.add(" or ")
    visit(b)
    sql.add(")")
  }

  ** visit 'isSpec'
  private Void visitIsSpec(Str spec)
  {
    specs++
    params.add(spec)

    // use a nested subquery
    sql.add("(exists (select 1 from spec s").add(specs).add(" where s").add(specs).add(".qname = r0.spec ")
    sql.add("and s").add(specs).add(".inherits_from = @x").add(x).add("))")
  }

  ** visit a leaf AST node
  private Void visitLeaf(
    FilterPath fp,
    |Int alias, Str path| nodeFunc)
  {
    // Create the paths using Haven's whitelisted ref paths
    paths := haven.refPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // process the node
      nodeFunc(0, paths[0])
    }
    // joins
    else
    {
      a := joins + 1
      joins += paths.size-1

      sql.add("(exists (")
      sql.add("select 1 from path_ref p").add(a).add(" ")
      sql.add("inner join rec r").add(a).add(" on r").add(a).add(".id = p").add(a).add(".target ")

      for (i := a+1; i <= joins; i++)
      {
        sql.add("inner join path_ref p").add(i).add(" on p").add(i).add(".source = r").add(i-1).add(".id ")
        sql.add("inner join rec r").add(i).add(" on r").add(i).add(".id = p").add(i).add(".target ")
      }

      sql.add("where (p").add(a).add(".source = r0.id) and ")
      for (i := 0; i < paths.size-1; i++)
      {
        params.add(paths[i])
        sql.add("(p").add(a+i).add(".path_ = @x").add(x).add(") and ")
      }
      // process the node
      nodeFunc(joins, paths[-1])

      sql.add("))")
    }
  }

  ** 'has' AST node
  private Void has(Int alias, Str path)
  {
    params.add("{\"$path\"}")
    sql.add("(r").add(alias).add(".paths @> @x").add(x).add("::text[])")
  }

  ** 'missing' AST node
  private Void missing(Int alias, Str path)
  {
    params.add("{\"$path\"}")
    sql.add("(not (r").add(alias).add(".paths @> @x").add(x).add("::text[]))")
  }

  ** 'eq' AST node
  private Void eq(Int alias, Str path, Obj? val)
  {
    // Misc toStr()
    if ((val is Str) || (val is Uri) || (val is Date) || (val is Time))
    {
      col := columnNames[val.typeof]

      addObjParam(path, val.toStr)
      sql.add("(r").add(alias).add(".").add(col).add(" @> @x").add(x).add("::jsonb)")
    }
    // Ref
    else if (val is Ref)
    {
      refEq(alias, path, val)
    }
    // Num
    else if (val is Number)
    {
      Number n := (Number) val

      addObjParam(path, n.toFloat)
      sql.add("((r").add(alias).add(".nums @> @x").add(x).add("::jsonb)")
      addObjParam(path, n.unit == null ? null : n.unit.toStr)
      sql.add(" and (r").add(alias).add(".units @> @x").add(x).add("::jsonb))")
    }
    // Bool
    else if (val is Bool)
    {
      addObjParam(path, val)
      sql.add("(r").add(alias).add(".bools @> @x").add(x).add("::jsonb)")
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val
      addObjParam(path, Duration(ts.ticks).toMillis)
      sql.add("(r").add(alias).add(".dateTimes @> @x").add(x).add("::jsonb)")
    }

    // val type cannot be used for this node
    else
      sql.add("false")
  }

  ** 'ne' AST node
  private Void ne(Int alias, Str path, Obj? val)
  {
    // Ref
    if (val is Ref)
    {
      sql.add("(not ")
      refEq(alias, path, val)
      sql.add(")")
    }
    // anything else
    else
    {
      col := columnNames[val.typeof]

      // We have to check if the column is null because of 3-Value booleans
      sql.add("(")
      has(alias, path)
      sql.add(" and ((r").add(alias).add(".").add(col).add(" is null) or (not ")
      eq(alias, path, val)
      sql.add(")))")
    }
  }

  ** test a ref for equality using a nested subquery
  private Void refEq(Int alias, Str path, Ref ref)
  {
    valRefs++

    sql.add("(exists (select 1 from path_ref v").add(valRefs).add(" ")
    sql.add("where v").add(valRefs).add(".source = r").add(alias).add(".id ")
    params.add(path)
    sql.add("and v").add(valRefs).add(".path_ = @x").add(x).add(" ")
    params.add(ref.id)
    sql.add("and v").add(valRefs).add(".target = @x").add(x).add("))")
  }

  ** 'cmp' AST node >,>=,<,<=
  private Void cmp(Int alias, Str path, Obj? val, Str op)
  {
    // https://hashrocket.com/blog/posts/dealing-with-nested-json-objects-in-postgresql
    // https://stackoverflow.com/questions/53841916/how-to-compare-numeric-in-postgresql-jsonb

    // Misc toStr()
    if ((val is Str) || (val is Date) || (val is Time))
    {
      col := columnNames[val.typeof]

      sql.add("(")
      has(alias, path)
      sql.add(" and ")

      params.add(path)
      sql.add("((r").add(alias).add(".").add(col).add(" ->> @x").add(x).add(")")
      params.add(val.toStr)
      sql.add(" ").add(op).add(" @x").add(x).add(")")

      sql.add(")")
    }
    // Number
    else if (val is Number)
    {
      Number n := (Number) val

      sql.add("(")
      has(alias, path)
      sql.add(" and ")

      params.add(path)
      sql.add("(((r").add(alias).add(".nums -> @x").add(x).add(")::real)")
      params.add(n.toFloat)
      sql.add(" ").add(op).add(" @x").add(x).add(")")

      addObjParam(path, n.unit == null ? null : n.unit.toStr)
      sql.add(" and (r").add(alias).add(".units @> @x").add(x).add("::jsonb)")

      sql.add(")")
    }
    // Bool
    else if (val is Bool)
    {
      sql.add("(")
      has(alias, path)
      sql.add(" and ")

      params.add(path)
      sql.add("(((r").add(alias).add(".bools -> @x").add(x).add(")::boolean)")
      params.add(val)
      sql.add(" ").add(op).add(" @x").add(x).add(")")

      sql.add(")")
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val

      sql.add("(")
      has(alias, path)
      sql.add(" and ")

      params.add(path)
      sql.add("(((r").add(alias).add(".dateTimes -> @x").add(x).add(")::bigint)")
      params.add(Duration(ts.ticks).toMillis)
      sql.add(" ").add(op).add(" @x").add(x).add(")")

      sql.add(")")
    }

    // val type cannot be used for this node
    else
      sql.add("false")
  }

  ** add a JSON Object param for a path:val pair
  private Void addObjParam(Str path, Obj? val)
  {
    params.add(
      JsonOutStream.writeJsonToStr(
        Str:Obj?[path:val]))
  }

  private Int x() { params.size-1 }

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
  private Obj[] params := Obj[,]

  private Int joins := 0
  private Int specs := 0
  private Int valRefs := 0
}


