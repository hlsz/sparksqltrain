package com.data.hadoop.hive;

import java.io.IOException;

import java.util.ArrayList;

import java.util.Collection;

import java.util.LinkedHashMap;

import java.util.Map;

import java.util.Stack;

import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;

import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;

import org.apache.hadoop.hive.ql.lib.Dispatcher;

import org.apache.hadoop.hive.ql.lib.GraphWalker;

import org.apache.hadoop.hive.ql.lib.Node;

import org.apache.hadoop.hive.ql.lib.NodeProcessor;

import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;

import org.apache.hadoop.hive.ql.lib.Rule;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;

import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.apache.hadoop.hive.ql.parse.ParseDriver;

import org.apache.hadoop.hive.ql.parse.ParseException;

import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HiveLineageInfo implements NodeProcessor {

    /**
     * Stores input tables in sql.
     */

    TreeSet<String> inputTableList = new TreeSet<String>();

    /**
     * Stores output tables in sql.
     */

    TreeSet<String> OutputTableList = new TreeSet<String>();

    /**
     * @return java.util.TreeSet
     */

    public TreeSet<String> getInputTableList() {

        return inputTableList;

    }

    /**
     * @return java.util.TreeSet
     */

    public TreeSet<String> getOutputTableList() {

        return OutputTableList;

    }

    /**
     * Implements the process method for the NodeProcessor interface.
     */

    @SuppressWarnings("rawtypes")

    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx,

                          Object... nodeOutputs) throws SemanticException {

        ASTNode pt = (ASTNode) nd;

        switch (pt.getToken().getType()) {

            case HiveParser.TOK_CREATETABLE:

                OutputTableList.add(BaseSemanticAnalyzer

                        .getUnescapedName((ASTNode) pt.getChild(0)));

                break;

            case HiveParser.TOK_TAB:

                OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));

                break;

            case HiveParser.TOK_TABREF:

                ASTNode tabTree = (ASTNode) pt.getChild(0);

                String table_name = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer

                        .getUnescapedName((ASTNode) tabTree.getChild(0))

                        : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree

                        .getChild(0)) + "." + tabTree.getChild(1);

                inputTableList.add(table_name);

                break;

        }

        return null;

    }

    /**
     * parses given query and gets the lineage info.
     *
     * @param query
     * @throws ParseException
     */

    public void getLineageInfo(String query) throws ParseException,

            SemanticException {

        /*

         * Get the AST tree

         */

        ParseDriver pd = new ParseDriver();

        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {

            tree = (ASTNode) tree.getChild(0);

        }

        /*

         * initialize Event Processor and dispatcher.

         */

        inputTableList.clear();

        OutputTableList.clear();

        // create a walker which walks the tree in a DFS manner while

        // maintaining

        // the operator stack. The dispatcher

        // generates the plan from the operator tree

        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        // The dispatcher fires the processor corresponding to the closest

        // matching

        // rule and passes the context along

        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);

        GraphWalker ogw = new DefaultGraphWalker(disp);

        // Create a list of topop nodes

        Collection<Node> topNodes = new ArrayList<Node>();

        topNodes.add(tree);

        ogw.startWalking(topNodes, null);

    }

    public static void main(String[] args) throws IOException, ParseException,

            SemanticException {

        //String query = "CREATE TABLE liuxiaowen.lxw1234 AS SELECT * FROM liuxiaowen.lxw1";

        String query = "INSERT OVERWRITE TABLE liuxiaowen.lxw3 SELECT a.url FROM liuxiaowen.lxw1 a join liuxiaowen.lxw2 b ON (a.url = b.domain)";

        HiveLineageInfo lep = new HiveLineageInfo();

        lep.getLineageInfo(query);

        System.out.println("Input tables = " + lep.getInputTableList());

        System.out.println("Output tables = " + lep.getOutputTableList());

    }

}
