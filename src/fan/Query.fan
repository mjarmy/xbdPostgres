//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Query
**
const class Query
{
  new make(Filter f)
  {
    sb := StrBuf()
    params := Obj[,]

    sb.add("select r.hayson from rec as r where ")
    visit(f, sb, params)

    this.sql = sb.toStr
    this.params = params
  }

  private static Void visit(
    Filter f, StrBuf sb, Obj[] params)
  {
    if (f.type == FilterType.has)
      visitHas(f.argA, sb, params)
    else
      throw Err("Encountered unknown FilterType ${f.type}")
  }

  private static Void visitHas(
    FilterPath fp, StrBuf sb, Obj[] params)
  {
    sb.add("(r.hayson ? '").add(fp.toStr).add("')")
  }

  //////////////////////////////////////////////////////////////
  // Obj
  //////////////////////////////////////////////////////////////

  override Int hash() { sql.hash.xor(params.hash) }

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
