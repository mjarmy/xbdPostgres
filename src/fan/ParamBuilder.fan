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
** ParamBuilder is just like QueryBuilder except it only finds the actual params.
**
internal class ParamBuilder {

  ** construct from filter
  internal new make(Haven haven, Filter f)
  {
    this.haven = haven
    visit(f)
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
        |Str alias, Str path| { has(alias, path) })

    case FilterType.missing:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { missing(alias, path) })

    case FilterType.eq:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { eq(alias, path, f.argB) })

    case FilterType.ne:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { ne(alias, path, f.argB) })

    case FilterType.lt:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { cmp(alias, path, f.argB) })

    case FilterType.le:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { cmp(alias, path, f.argB) })

    case FilterType.gt:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { cmp(alias, path, f.argB) })

    case FilterType.ge:
      visitLeaf(
        (FilterPath)f.argA,
        |Str alias, Str path| { cmp(alias, path, f.argB) })

    default:
        throw Err("Encountered unknown FilterType ${f.type}")
    }
  }

  ** visit 'and'
  private Void visitAnd(Filter a, Filter b)
  {
    visit(a)
    visit(b)
  }

  ** visit 'or'
  private Void visitOr(Filter a, Filter b)
  {
    visit(a)
    visit(b)
  }

  ** visit 'isSpec'
  private Void visitIsSpec(Str spec)
  {
    addParam(spec)
  }

  ** visit a leaf AST node
  private Void visitLeaf(
    FilterPath fp,
    |Str alias, Str path| nodeFunc)
  {
    // Create the paths using Haven's whitelisted ref paths
    paths := haven.refPaths(fp)

    // no joins
    if (paths.size == 1)
    {
      // process the node
      nodeFunc("rec", paths[0])
    }
    // joins
    else
    {
      a := joins + 1
      joins += paths.size-1
      for (i := 0; i < paths.size-1; i++)
      {
        addParam(paths[i])
      }
      // process the node
      nodeFunc("r${joins}", paths[-1])
    }
  }

  ** 'has' AST node
  private Void has(Str alias, Str path)
  {
    addParam("{\"$path\"}")
  }

  ** 'missing' AST node
  private Void missing(Str alias, Str path)
  {
    addParam("{\"$path\"}")
  }

  ** 'eq' AST node
  private Void eq(Str alias, Str path, Obj? val)
  {
    // Misc toStr()
    if ((val is Str) || (val is Uri) || (val is Date) || (val is Time))
    {
      addObjParam(path, val.toStr)
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
      addObjParam(path, n.toFloat)
      addObjParam(path, n.unit == null ? null : n.unit.toStr)
    }
    // Bool
    else if (val is Bool)
    {
      addObjParam(path, val)
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val
      addObjParam(path, Duration(ts.ticks).toMillis)
    }

    // val type cannot be used for this node
    else
      throw Err("unreachable")
  }

  ** 'ne' AST node
  private Void ne(Str alias, Str path, Obj? val)
  {
    // Ref
    if (val is Ref)
    {
      refEq(alias, path, val)
    }
    // anything else
    else
    {
      has(alias, path)
      eq(alias, path, val)
    }
  }

  ** test a ref for equality using a nested subquery
  private Void refEq(Str alias, Str path, Ref ref)
  {
    addParam(path)
    addParam(ref.id)
  }

  ** 'cmp' AST node >,>=,<,<=
  private Void cmp(Str alias, Str path, Obj? val)
  {
    // Misc toStr()
    if ((val is Str) || (val is Date) || (val is Time))
    {
      has(alias, path)

      addParam(path)
      addParam(val.toStr)
    }
    // Number
    else if (val is Number)
    {
      Number n := (Number) val

      has(alias, path)

      addParam(path)
      addParam(n.toFloat)
      addObjParam(path, n.unit == null ? null : n.unit.toStr)
    }
    // Bool
    else if (val is Bool)
    {
      has(alias, path)

      addParam(path)
      addParam(val)
    }
    // DateTime
    else if (val is DateTime)
    {
      DateTime ts := (DateTime) val

      has(alias, path)

      addParam(path)
      addParam(Duration(ts.ticks).toMillis)
    }

    // val type cannot be used for this node
    else
      throw Err("unreachable")
  }

  ** add a JSON Object param for a path:val pair
  private Void addObjParam(Str path, Obj? val)
  {
    addParam(
      JsonOutStream.writeJsonToStr(
        Str:Obj?[path:val]))
  }

  ** add a param
  private Void addParam(Obj val)
  {
    name := "x${params.size}"
    params.add(name, val)
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private Haven haven

  Str:Obj params := Str:Obj[:]

  private Int joins := 0
}


