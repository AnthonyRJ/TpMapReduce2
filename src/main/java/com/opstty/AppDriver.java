package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("roundingContainingTrees", RoundingContainingTrees.class,
                    "A map/reduce program that shows how many trees are in Paris roundings");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program displays the list of different species trees");
            programDriver.addClass("treesByKinds", NumberTreesByKinds.class,
                    "A map/reduce program that calculates the number of trees of each kinds");
            programDriver.addClass("maxHeight", MaximumHeightPerKind.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind");
            programDriver.addClass("treesSmallToLarge", TreesSmallToLarge.class,
                    "A map/reduce program that sort the trees from smallest to largest");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
