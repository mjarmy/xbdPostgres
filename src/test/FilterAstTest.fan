//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   10 June 2024  Mike Jarmy  Creation
//

using haystack

**
** FilterAstTest
**
class FilterAstTest : Test
{
  Void test()
  {
    doTest("a",              AstType.markers)
    doTest("fooRef == @abc", AstType.refEq)
    doTest("x < 42",         AstType.adHoc)
  }

  private Void doTest(Str f, AstType t)
  {
    echo(f)
    verifyEq(FilterAst(Filter(f)).type, t)
  }
}
