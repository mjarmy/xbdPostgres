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
    doTest("a",                 AstType.markers)
    doTest("a and b",           AstType.markers)
    doTest("a and b and c",     AstType.markers)

    doTest("fooRef == @abc",    AstType.ref)

    doTest("foo::Bar",          AstType.spec)

    doTest("a or b",            AstType.adHoc)
    doTest("a and b and not c", AstType.adHoc)
    doTest("x < 42",            AstType.adHoc)
  }

  private Void doTest(Str f, AstType t)
  {
    echo("---> $f")
    verifyEq(FilterAst(Filter(f)).type, t)
  }
}
