package ch.ethz.systems.strymon.ds2.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

public class RandomSentenceGenerator implements Serializable {
  private Random rand;
  private ArrayList<String> wordList;

  public RandomSentenceGenerator() {
    rand = new Random();
    try {
      wordList = prepareWordList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ArrayList<String> prepareWordList() throws IOException {
    ArrayList<String> wordList = new ArrayList<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/words.txt")));
    String line;
    while ((line = reader.readLine()) != null) {
      wordList.add(line + " ");
    }
    return wordList;
  }

  public String nextSentence(int desiredSentenceSize) {
    return nextSentence(desiredSentenceSize, 0);
  }

  public String nextSentence(int desiredSentenceSize, int skewPercent) {
    StringBuilder builder = new StringBuilder();
    while (desiredSentenceSize > 0) {
      String word = nextWord(skewPercent);
      desiredSentenceSize -= word.length();
      builder.append(word);
    }
    return builder.toString();
  }

  public String nextWord(int skewPercent) {
    if (skewPercent > 0 && rand.nextInt(100) < skewPercent) {
      return "skew ";
    } else {
      return wordList.get(rand.nextInt(wordList.size()));
    }
  }
}
