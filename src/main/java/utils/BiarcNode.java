package utils;

import java.util.ArrayList;
import java.util.List;

public class BiarcNode {
    private String word;
    private String tag;
    private int parent;
    private List<BiarcNode> children;

    public BiarcNode(String word, String tag, int parent){
        Stemmer stemmer = new Stemmer();
        stemmer.add(word.toCharArray(),word.length());
        stemmer.stem();
        this.word = stemmer.toString();
        this.tag = tag;
        this.parent=parent;
        children = new ArrayList<>();
    }

    public boolean isNoun(){
        return tag.contains("NN");
    }
    public void addChild(BiarcNode c){
        children.add(c);
    }

    public String getWord() {
        return word;
    }

    public String getTag() {
        return tag;
    }

    public int getParent() {
        return parent;
    }

    public List<BiarcNode> getChildren() {
        return children;
    }
}
