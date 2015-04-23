package com.betterdocs.parser;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.BodyDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.MultiTypeParameter;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.expr.VariableDeclarationExpr;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.CatchClause;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.type.Type;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;

@SuppressWarnings("rawtypes")
public class JavaFileParser extends VoidVisitorAdapter {

	private List<ImportDeclaration> imports;
	private Map<String, String> importDeclMap;
	private String className;
	private String url;
	private HashMap<String, String> nameVsTypeMap;
	private String currentMethodlines;
	private Integer score;
	private Map<String, String> fieldVariableMap = new HashMap<String, String>();
	private Map<String, List<String>> methodCallStack = new HashMap<String, List<String>>();
	private String declaredPackage;

	public void parse(String classcontent, String url, Integer score) throws ParseException, IOException {
		if (classcontent == null || classcontent.isEmpty()) {
			System.err.println("No class content to parse... " + url);
		}
		if (url == null || url.isEmpty()) {
			System.err.println("No class url - skipping this" );
			return;
		}
		this.url = url;
		this.score = score;
		try {
			parse(new ByteArrayInputStream(classcontent.getBytes()));
		} catch (Throwable e) {
			//System.err.println("Could not parse. Skipping file: " + url + ", exception: " + e);
			//e.printStackTrace(System.err);
			return;
		}
		
		//System.out.println("Parsed file : " + url);
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
			declaredPackage = pakg!=null?pakg.getName().toString():pkg;
			setImports(imports);
			visit(cu, declaredPackage);

		}
	}

	@Override
	public void visit(ClassOrInterfaceDeclaration n, Object arg) {
		className = arg + "." + n.getName();
		List<BodyDeclaration> members = n.getMembers();
		for (BodyDeclaration b : members) {
			if (b instanceof FieldDeclaration) {
				visit((FieldDeclaration) b, null);
			} else if (b instanceof MethodDeclaration) {
				visit((MethodDeclaration) b, null);
			}
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
		//currentMethod = className + "." + n.getName();
		currentMethodlines = n.getBeginLine() +"-" + n.getEndLine();
		if (body != null) {
			visit(body, nameVsTypeMap);
		}
	}

	@Override
	public void visit(ExpressionStmt n, Object arg) {
		Expression xpr = n.getExpression();
		if (xpr instanceof MethodCallExpr) {
			visit((MethodCallExpr) xpr, arg);
		} else if (xpr instanceof AssignExpr) {
			visit((AssignExpr) xpr, arg);
		} else if (xpr instanceof VariableDeclarationExpr) {
			visit((VariableDeclarationExpr) xpr, arg);
		} else if (xpr instanceof ObjectCreationExpr) {
			visit((ObjectCreationExpr) xpr, arg);
		}
	}

	@Override
	public void visit(AssignExpr n, Object arg) {
		//TODO: Replace the declared type with the actual type etc.
	}
	
	@Override
	public void visit(ObjectCreationExpr n, Object arg1) {
		//TODO:
	}
	

	@Override
	public void visit(MethodCallExpr n, Object arg) {
		Expression s = n.getScope();
		if (s instanceof MethodCallExpr) {
			MethodCallExpr m = (MethodCallExpr) s;
			visit(m, arg);
			// can't get the return type so atleast salvage on call.
			return;
		}
		String scope = "this";
		if (s != null) {
			scope = s.toString();
		}
		String name = n.getName();
		String fullscope = nameVsTypeMap.get(scope);

		// if scope is null, then static call or field decl
		if (fullscope == null)
			fullscope = fieldVariableMap.get(scope);
		if (fullscope == null)
			fullscope = importDeclMap.get(scope);
		if (fullscope == null){
			//fullscope = scope;
			//ignore the java.lang or package local calls -- not very significant
			//TODO: Handle java.lang later
			return;
		}
		
		String methodTransactionKey = getMethodTransactionKey();
		List<String> stack = methodCallStack.get(methodTransactionKey);
		if (stack == null) {
			stack = new ArrayList<String>();
			methodCallStack.put(methodTransactionKey, stack);
		}

		String call = fullscope + "." + name;
		boolean isArrayCall = call.contains("[") && call.contains("]");
		if (!isArrayCall) {
			stack.add(call);
		}
		
	}

	private String getMethodTransactionKey() {
		return url+"#L"+currentMethodlines+"#"+ score;
	}

	@Override
	public void visit(VariableDeclarationExpr n, Object arg) {
		//System.out.println("\t \t Variable declare : " + n );
		List<VariableDeclarator> vars = n.getVars();
		for (VariableDeclarator v : vars) {
			String id = v.getId().toString();
			Expression initExpr = v.getInit();
			if(initExpr instanceof MethodCallExpr){
				visit((MethodCallExpr) initExpr, arg);
			}
			String type = n.getType().toString();
			nameVsTypeMap.put(id, fullType(type));
		}

	}

	private String fullType(String type) {
		
		String typeStr = type.toString();
		if(typeStr.contains("<") && typeStr.contains(">")){
			typeStr = typeStr.substring(0, typeStr.indexOf("<"));
		}
		String fullType = importDeclMap.get(typeStr);
		fullType = fullType == null ? null : fullType;
		return fullType;
	}

	public List<ImportDeclaration> getImports() {
		return imports;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void visit(CatchClause n, Object arg) {
		MultiTypeParameter exception = n.getExcept();

		List<Type> types = exception.getTypes();

		//Can't handle multitype properly.
		if (types != null && types.size() == 1) {
			Type type = types.get(0);
			String name = exception.getId().getName();
			nameVsTypeMap.put(name, fullType(type.toString()));

			if (n != null) {
				visit(n.getCatchBlock(), arg);
			}

		}
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

	public Map<String, List<String>> getMethodCallStack() {
		return methodCallStack;
	}

	public void setMethodCallStack(Map<String, List<String>> methodCallStack) {
		this.methodCallStack = methodCallStack;
	}
	
	public String getDeclaredPackage() {
		return declaredPackage;
	}

	public void setDeclaredPackage(String declaredPackage) {
		this.declaredPackage = declaredPackage;
	}
	
	public Set<String> gedUsedPackages(){
		HashSet<String> result = new HashSet<String>();
		if(importDeclMap!=null && !importDeclMap.isEmpty()){
			for(String imp : importDeclMap.values()){
				int lastIndexOf = imp.lastIndexOf(".");
				if(lastIndexOf > 0){
					String pkg = imp.substring(0, lastIndexOf);
					if(pkg != null && !pkg.isEmpty())
						result.add(pkg);	
				}
			}
		}
		return result;
	}

	/**
	 * **** Remove 
	 */
	
	public static void main(String[] args) throws Throwable {
		// creates an input stream for the file to be parsed
		/*String test = "/home/sachint/work/Test.java";
		String test1 = "/home/sachint/work/Test1.java";
		
		FileInputStream in = new FileInputStream(
				test);

		JavaFileParser m = new JavaFileParser();
		m.parse(in);
		Map<String, List<String>> callstcak = m.getMethodCallStack();
		for(Entry<String, List<String>> e: callstcak.entrySet()){
			String key = e.getKey();
			System.out.println("Method11 [ " + key + " ]");
			
			for(String call: e.getValue()){
				System.out.println("\t " + call );
			}
		}
		
		System.out.println(m.gedUsedPackages());
		System.out.println(m.getDeclaredPackage());*/
		
		String s = "org.apache.synapse.config.Entry.isDynamic";
		Pattern p = Pattern.compile("([a-zA-Z]+.*)\\.([a-zA-Z]*)", Pattern.CASE_INSENSITIVE);
		ArrayList<String> result = new ArrayList<String>();
		getMatches(s, p, result);
		System.out.println(result);

	}

	private static void getMatches(String s, Pattern p, List<String> result) {
		Matcher matcher = p.matcher(s);
		if(matcher.find()){
			String group = matcher.group(1);
			result.add(group);
			getMatches(group, p, result);
		}
		
	}

}
