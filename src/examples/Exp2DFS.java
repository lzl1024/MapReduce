package examples;

import util.Constants;
import dfs.DFSApi;

public class Exp2DFS {
    public static void main(String[] args) throws Exception {
        new Constants(args[0]);
        DFSApi.put(args[1]);
        DFSApi.get(args[1], args[2], false);
        DFSApi.delete(args[1]);
    }
}
