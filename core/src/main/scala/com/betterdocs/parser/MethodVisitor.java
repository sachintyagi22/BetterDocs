/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.betterdocs.parser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.CatchClause;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.type.Type;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class MethodVisitor extends VoidVisitorAdapter {

    private List<ImportDeclaration> imports;
    private Map<String, String> importDeclMap;
    private Map<String, String> fieldVariableMap = new HashMap<String, String>();
    private String className = null;
    private Map<String, List<String>> methodCallStack = new HashMap<String, List<String>>();
    private String currentMethod;
    private HashMap<String, String> nameVsTypeMap = new HashMap<String, String>();
    private HashMap<String, ArrayList<Integer>> lineNumbersMap = new HashMap<String, ArrayList<Integer>>();
    private ArrayList<HashMap<String, ArrayList<Integer>>> listOflineNumbersMap = new
            ArrayList<HashMap<String, ArrayList<Integer>>>();

    public void parse(String classcontent, String filename) throws Throwable {
        if (classcontent == null || classcontent.isEmpty()) {
            System.err.println("No class content to parse... " + filename);
        }
        try {
            parse(new ByteArrayInputStream(classcontent.getBytes()));
        } catch (Throwable e) {
            //System.err.println("Could not parse. Skipping file: " + filename + ", exception: " +
            //        e.getMessage());
            throw e;
        }

        //System.out.println("Parsed file : " + filename);
    }

    @SuppressWarnings("unchecked")
    public void parse(InputStream in) throws Throwable {
        CompilationUnit cu = null;
        try {
            // parse the file
            cu = JavaParser.parse(in);
        } catch (Throwable e) {
            throw e;
        } finally {
            in.close();
        }

        if (cu != null) {
            List<ImportDeclaration> imports = cu.getImports();
            PackageDeclaration pakg = cu.getPackage();
            String pkg = "";
            pkg = pakg != null ? pakg.getName().toString() : pkg;
            setImports(imports);
            visit(cu, pkg);
        }
    }

    @Override
    public void visit(ClassOrInterfaceDeclaration n, Object arg) {
        className = arg + "." + n.getName();
        List<BodyDeclaration> members = n.getMembers();
        for (BodyDeclaration b : members) {
            fancyVisitBody(null, b);
        }
    }

    @Override
    public void visit(FieldDeclaration n, Object arg) {
        List<VariableDeclarator> variables = n.getVariables();
        for (VariableDeclarator v : variables) {
            Type type = n.getType();
            fieldVariableMap.put(v.getId().toString(),
                    fullType(type.toString()));
        }
    }

    @Override
    public void visit(ConstructorDeclaration n, Object arg) {
        nameVsTypeMap = new HashMap<String, String>();
        List<Parameter> parameters = n.getParameters();
        if (parameters != null) {
            for (Parameter p : parameters) {
                String type = p.getType().toString();
                nameVsTypeMap.put(p.getId().toString(), fullType(type));
            }
        }

        nameVsTypeMap.put("this", className);
        BlockStmt body = n.getBlock();
        currentMethod = className + "." + n.getName();
        if (body != null) {
            visit(body, nameVsTypeMap);
        }
        // On each method encountered we store their imports as map.
        listOflineNumbersMap.add(lineNumbersMap);
        lineNumbersMap = new HashMap<String, ArrayList<Integer>>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void visit(MethodDeclaration n, Object arg) {
        nameVsTypeMap = new HashMap<String, String>();
        List<Parameter> parameters = n.getParameters();
        if (parameters != null) {
            for (Parameter p : parameters) {
                String type = p.getType().toString();
                nameVsTypeMap.put(p.getId().toString(), fullType(type));
            }
        }

        nameVsTypeMap.put("this", className);
        BlockStmt body = n.getBody();
        currentMethod = className + "." + n.getName();
        if (body != null) {
            visit(body, nameVsTypeMap);
        }
        // On each method encountered we store their imports as map.
        listOflineNumbersMap.add(lineNumbersMap);
        lineNumbersMap = new HashMap<String, ArrayList<Integer>>();
    }

    public void fancyVisit(Expression exp, Object arg) {
        if (exp instanceof MethodCallExpr) {
            visit((MethodCallExpr) exp, arg);
        } else if (exp instanceof AssignExpr) {
            visit((AssignExpr) exp, arg);
        } else if (exp instanceof VariableDeclarationExpr) {
            visit((VariableDeclarationExpr) exp, arg);
        } else if (exp instanceof ObjectCreationExpr) {
            visit((ObjectCreationExpr) exp, arg);
        } else if (exp instanceof CastExpr) {
            visit((CastExpr) exp, arg);
        } else if (exp instanceof EnclosedExpr) {
            visit((EnclosedExpr) exp, arg);
        } else if (exp instanceof FieldAccessExpr) {
            visit((FieldAccessExpr) exp, arg);
        } else if (exp != null && !getFullScope(exp).equals(exp.toString())) {
            updateLineNumbersMap(getFullScope(exp), exp.getBeginLine());
        }
    }

    @Override
    public void visit(ExpressionStmt n, Object arg) {
        Expression xpr = n.getExpression();
        if (xpr != null) fancyVisit(xpr, arg);
    }

    @Override
    public void visit(FieldAccessExpr n, Object arg) {
        Expression s = n.getScope();
        if (s != null) fancyVisit(s, arg);
        fancyVisit(n.getFieldExpr(), arg);
    }

    @Override
    public void visit(AssignExpr n, Object arg) {
        if (n != null) {
            try {
                Expression target = n.getTarget();
                Expression value = n.getValue();
                fancyVisit(target, arg);
                fancyVisit(value, arg);
//                String targetScope = getFullScope(target);
//                String valueScope = getFullScope(value);
//                //lines.add(target.getEndLine()); TODO: Maybe add this ?
//                updateLineNumbersMap(targetScope, target.getBeginLine());
//                updateLineNumbersMap(valueScope, value.getBeginLine());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void updateLineNumbersMap(String targetScope, Integer line) {
        ArrayList<Integer> lineNumbers = lineNumbersMap.get(targetScope);
        if (lineNumbers == null) {
            lineNumbers = new ArrayList<Integer>();
        }
        lineNumbers.add(line);
        lineNumbersMap.put(targetScope, lineNumbers);
    }

    @Override
    public void visit(ObjectCreationExpr n, Object arg1) {

        if (n != null) {
            try {
                String fullTypeName = fullType(n.getType().toString());
                if (n.getArgs() != null) {
                    for (Expression arg : n.getArgs()) {
                        fancyVisit(arg, arg1);
                    }
                }
                updateLineNumbersMap(fullTypeName, n.getBeginLine());

                // Process anonymous class body.
                if (n.getAnonymousClassBody() != null) {
                    for (BodyDeclaration bdecl : n.getAnonymousClassBody()) {
                        specialVisitBody(bdecl, arg1); // special visit body

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void fancyVisitBody(Object arg1, BodyDeclaration bdecl) {
        if (bdecl instanceof FieldDeclaration) {
            visit(((FieldDeclaration) bdecl), arg1);
        } else if (bdecl instanceof MethodDeclaration) {
            visit((MethodDeclaration) bdecl, arg1);
        } else if (bdecl instanceof ClassOrInterfaceDeclaration) {
            visit((ClassOrInterfaceDeclaration) bdecl, arg1);
        } else if (bdecl instanceof ConstructorDeclaration) {
            visit((ConstructorDeclaration) bdecl, arg1);
        }
    }

    // used for anonymous class bodies only
    private void specialVisitBody(BodyDeclaration bdecl, Object arg1) {
        if (bdecl instanceof FieldDeclaration) {
            visit(((FieldDeclaration) bdecl), arg1);
        } else if (bdecl instanceof MethodDeclaration) {
            MethodDeclaration n = (MethodDeclaration) bdecl;
            List<Parameter> parameters = n.getParameters();
            if (parameters != null) {
                for (Parameter p : parameters) {
                    String type = p.getType().toString();
                    nameVsTypeMap.put(p.getId().toString(), fullType(type));
                }
            }
            BlockStmt body = n.getBody();
            if (body != null) {
                visit(body, nameVsTypeMap);
            }
        }
    }

    @Override
    public void visit(MethodCallExpr n, Object arg) {
        Expression s = n.getScope();
        List<Expression> args = n.getArgs();
        if (args != null) {
            for (Expression e : args) {
                fancyVisit(e, arg);
            }
        }
        fancyVisit(s, arg);
        //updateCallStack(s, n.getName());
    }

    private void updateCallStack(Expression s, String name) {
        String fullscope = getFullScope(s);
        if (s != null) {
            updateLineNumbersMap(fullscope, s.getBeginLine());
        }
        List<String> stack = methodCallStack.get(currentMethod);
        if (stack == null) {
            stack = new ArrayList<String>();
            methodCallStack.put(currentMethod, stack);
        }

        String call = fullscope + "." + name;
        boolean isArrayCall = call.contains("[") && call.contains("]");
        if (!isArrayCall) {
            stack.add(call);
        }
    }

    private String getFullScope(Expression s) {
        String scope = "this";
        if (s != null) {
            scope = s.toString();
        }
        String fullscope = nameVsTypeMap.get(scope);

        // if scope is null, then static call or field decl
        if (fullscope == null)
            fullscope = fieldVariableMap.get(scope);
        if (fullscope == null)
            fullscope = importDeclMap.get(scope);
        if (fullscope == null)
            fullscope = scope;
        return fullscope;
    }

    @Override
    public void visit(VariableDeclarationExpr n, Object arg) {
        //System.out.println("\t \t Variable declare : " + n );
        List<VariableDeclarator> vars = n.getVars();
        for (VariableDeclarator v : vars) {
            String id = v.getId().toString();
            Expression initExpr = v.getInit();
            fancyVisit(initExpr, arg);
            String type = n.getType().toString();
            nameVsTypeMap.put(id, fullType(type));
            updateLineNumbersMap(fullType(type), n.getBeginLine());
        }

    }

    @Override
    public void visit(CastExpr n, Object arg) {
        updateLineNumbersMap(fullType(n.getType().toString()), n.getBeginLine());
        if (n.getExpr() != null) fancyVisit(n.getExpr(), arg);
    }

    @Override
    public void visit(EnclosedExpr expr, Object arg) {
        if (expr.getInner() != null) fancyVisit(expr.getInner(), arg);
    }

    private String fullType(String type) {
        String fullType = importDeclMap.get(type.toString());
        fullType = fullType == null ? type.toString() : fullType;
        return fullType;
    }

    public List<ImportDeclaration> getImports() {
        return imports;
    }

    public void setImports(List<ImportDeclaration> imports) {
        this.imports = imports;
        importDeclMap = new HashMap<String, String>();
        if (imports != null) {
            for (ImportDeclaration d : imports) {
                String name = d.getName().toString();

                String[] tokens = name.split("\\.");
                if (tokens != null && tokens.length > 0) {
                    importDeclMap.put(tokens[tokens.length - 1], name);
                }
            }

        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void visit(CatchClause n, Object arg) {
        MultiTypeParameter exception = n.getExcept();

        String name = exception.getId().getName();
        exception.accept(this, arg);
        for (Type t : exception.getTypes()) {
            nameVsTypeMap.put(name, fullType(t.toString()));
            updateLineNumbersMap(fullType(t.toString()), exception.getBeginLine());
        }

        if (n != null) {
            visit(n.getCatchBlock(), arg);
        }
    }

    public Map<String, List<String>> getMethodCallStack() {
        return methodCallStack;
    }

    public ArrayList<HashMap<String, ArrayList<Integer>>> getListOflineNumbersMap() {
        return listOflineNumbersMap;
    }

    public Map<String, String> getImportDeclMap() {
        return importDeclMap;
    }
}
