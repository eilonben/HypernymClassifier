package utils;

public class DepGraph {
    public static BiarcNode getGraphRoot(String[] graph) {
        BiarcNode[] nodes = new BiarcNode[graph.length];
        for (int i = 0; i < graph.length; i++) {
            String[] node = graph[i].split("/");
            if (node.length != 4) {
                return null;
            }
            String word1 = node[0].replaceAll("[^a-zA-Z]+", "");
            String tag = node[1].replaceAll("[^a-zA-Z]+", "");
            if (word1.equals("") || tag.equals("")) {
                return null;
            }
            int parent = 0;
            try {
                parent = Integer.parseInt(node[3]);
            } catch (Exception e) {
            }
            nodes[i] = new BiarcNode(word1, tag, parent);
        }
        int root = 0;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].getParent() > 0)
                nodes[nodes[i].getParent() - 1].addChild(nodes[i]);
            else
                root = i;
        }
        return nodes[root];
    }


}
