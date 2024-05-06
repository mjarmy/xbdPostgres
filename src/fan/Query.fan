//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

****************************************************************
** Query
****************************************************************

const class Query
{
  new make(Filter f)
  {
    qb := QueryBuilder(f)
    this.sql = qb.sqlBuf.toStr
    this.params = qb.params
  }

  //////////////////////////////////////////////////////////////
  // Obj
  //////////////////////////////////////////////////////////////

  //override Int hash() { sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Query
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Query('$sql', $params)" }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  const Str sql
  const Obj[] params
}

****************************************************************
** QueryBuilder
****************************************************************

internal class QueryBuilder {

  new make(Filter f)
  {
    this.sqlBuf = StrBuf()
    this.params = Obj[,]

    sqlBuf.add("select r.hayson from rec as r where ")
    visit(f)
  }

  private Void visit(Filter f)
  {
    if      (f.type == FilterType.has) visitHas(f.argA)
    else if (f.type == FilterType.eq)  visitEq(f.argA, f.argB)
    else if (f.type == FilterType.and) visitAnd(f.argA, f.argB)
    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  private Void visitHas(FilterPath path)
  {
    sqlBuf
      .add("(r.hayson ? '")
      .add(path.toStr)
      .add("')")
  }

  private Void visitEq(FilterPath path, Obj arg)
  {
    sqlBuf
      .add("(r.hayson @> '{\"")
      .add(path.toStr)
      .add("\":?}'::jsonb)")

    params.add(arg)
  }

  private Void visitAnd(Filter a, Filter b)
  {
    sqlBuf.add("(");
    visit(a)
    sqlBuf.add(" and ");
    visit(b)
    sqlBuf.add(")");
  }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  internal StrBuf sqlBuf
  internal Obj[] params
}


