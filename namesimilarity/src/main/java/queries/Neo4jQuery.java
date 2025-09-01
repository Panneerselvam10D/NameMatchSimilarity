package queries;

import org.apache.commons.codec.language.Soundex;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.neo4j.driver.Record;


import org.neo4j.driver.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;


public class Neo4jQuery {

    public static void main(String[] args) {
        double threshold = 80.00;

//        String InputExcelFilePath = "/home/decoders/Music/TEST_3000.xlsx";
        if (args.length < 2) {
            System.out.println("Usage: java -jar myapp.jar <inputFile> <outputDir>");
            System.exit(1);
        }

        String InputExcelFilePath = args[0];
        String outputDir = args[1];

//        String inputName = "Oleksander Kostyantynovych Akimov";
//		  DoNameSimilarityAgainstSDNDB(inputName, threshold);


        try {
            FileInputStream fis = new FileInputStream(new File(InputExcelFilePath));
            Workbook workbook = new XSSFWorkbook(fis);
            Sheet sheet = workbook.getSheetAt(0);
            Integer times = 1;
            for (Row row : sheet) {
                Cell cell = row.getCell(0);
                if (cell != null) {
                    String inputName = cell.getStringCellValue().trim();
                    System.out.println("Processing: " + inputName);
//					if(times > 3) {
//						break;
//					}

                    try {
                        DoNameSimilarityAgainstSDNDB(inputName, threshold,outputDir);
                    }catch(Exception e) {
                        System.out.println("Exception For The Following Input : " +  inputName);
                    }
                    times++;
                }
            }

            workbook.close();
            fis.close();

            System.out.println("Neo4j Results Write Completed In Excel");

        } catch (IOException e) {
            e.printStackTrace();
        }




    }







    private static final String URI = "bolt://194.163.150.172:7687"; // 194.163.150.172 or 38.242.220.73:7687
    private static final String UserName = "neo4j";
    private static final String Password = "password";

    private static final double FIRSTNAME_WEIGHT = 0.75;
    private static final double LASTNAME_WEIGHT = 1.00;
    private static final double MIDDLENAME_WEIGHT = 0.5;

    private static final double PENALTY_WEIGHT = 0.95;
    private static final double BEST_SCORE_THRESHOLD = 0.60;
    private static final double UNMATCHED_TOKEN_SIMILARITY = 0.75;

    // Lower the repeating element penalty to be less aggressive
    private static final double REPEATING_ELEMENT_PENALTY = 0.97;

    // Add a minimum component weight to prevent over-penalization
    private static final double MIN_COMPONENT_WEIGHT = 0.3;

    // New: Better weight for long complex name matches
    private static final double COMPLEX_NAME_BONUS = 1.05;

    private static final double SDN_PENALTY_WEIGHT = 0.05;

    private static final double INPUT_PENALTY_WEIGHT = 0.07;

    private static final double SDN_PENALTIES_WEIGHT = 0.05;

    private static final double SDN_REPEATING_PENALTY_WEIGHT = 0.02;

    private static final double LASTNAME_DENOMINATOR = 0.2;

    // Example set of "stopwords" or "noise" tokens to ignore or de-weight
    private static final List<String> STOPWORDS = Arrays.asList("al", "bin", "binti", "b.", "bte", "bt", "a/l", "a/p",
            "s/o", "d/o", "so", "do");

    private static Driver driver;

    private static void Connect() {
        driver = GraphDatabase.driver(Neo4jQuery.URI, AuthTokens.basic(Neo4jQuery.UserName,
                Neo4jQuery.Password));
    }
    private static void close() {
        driver.close();
    }

    private static String escapeLuceneQuery(String query) {
        if (query == null || query.isEmpty()) {
            return query;
        }

        // List of characters to escape in Lucene
        String[] reservedCharacters = {
                "\\", "+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]",
                "^", "\"", "~", "*", "?", ":", "/"
        };

        // Escape each reserved character
        for (String ch : reservedCharacters) {
            // Use replaceAll with regex escaping for the character
            query = query.replace(ch, "\\" + ch);
        }
        return query;
    }


    public static List<SDNNameList> pullNamesFromDatabase(String inputName, String type, String fileType, double nmp) {
        validateNotEmpty(inputName, "fullName");
        validateNotEmpty(type, "type");
        validateNotEmpty(fileType, "fileType");
        List<SDNNameList> sdnNamesLists = new ArrayList<SDNNameList>();
        String personWLQuery =
                "CALL db.index.fulltext.queryNodes(\"activeFullNameFullTextIndex\", $searchName) YIELD node , score " +
                        "WHERE score > 1.75 " +
                        "RETURN node.fullName as title, node.firstName as firstName, node.middleName as middleName, " +
                        "node.surname as surname, node.recordId as recordId";

        String socPersonQuery =
                "CALL db.index.fulltext.queryNodes(\"activeSOCFullNameFullTextIndex\", $searchName) YIELD node , score " +
                        "WHERE score > 1.75 " +
                        "RETURN node.fullName as title, node.firstName as firstName, node.middleName as middleName, " +
                        "node.surname as surname, node.recordId as recordId";

        //Neo4J QueryParser.escape handling escape character
        inputName = escapeLuceneQuery(inputName);

        if (nmp != 100.0) {
            personWLQuery = personWLQuery.replace("$searchName", "\"" + inputName + "~0.5\"");
            socPersonQuery = socPersonQuery.replace("$searchName", "\"" + inputName + "~0.5\"");

        }

        String selectedQuery = null;

        // Determine query based on type and fileType
        if ("Person".equalsIgnoreCase(type)) {
            if ("WATCH_LIST".equalsIgnoreCase(fileType)) {
                selectedQuery = personWLQuery;
            } else if ("STATE_OWNED".equalsIgnoreCase(fileType)) {
                selectedQuery = socPersonQuery;
            } else {
                throw new IllegalArgumentException("Unsupported Person fileType combination.");
            }
        } else {
            throw new IllegalArgumentException("Unsupported type combination.");
        }

        if (driver == null || driver.session().isOpen()) {
            Connect();
        }
        SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase( "neo4j" )
                .withFetchSize( 10000 )
                .build();

        try (Session session = driver.session(sessionConfig)) {
            long startTime = System.currentTimeMillis();
            String finalQuery = selectedQuery;
            session.executeRead(tx -> {
                Result result = tx.run(finalQuery);
                while (result.hasNext()) {
                    Record record = result.next();
                    Value firstName = record.get("firstName");
                    Value middleName = record.get("middleName");
                    Value lastName = record.get("surname");
                    Value fullName = record.get("title");
                    Value recordId = record.get("recordId");

                    String strFirstName = firstName.isNull() ? "" : firstName.asString();
                    String strMiddleName = middleName.isNull() ? "" : middleName.asString();
                    String strLastName = lastName.isNull() ? "" : lastName.asString();
                    String strFullName = fullName.isNull() ? "" : fullName.asString();
                    String strRecordId = recordId.isNull() ? "" : recordId.asString();

                    SDNNameList sdnNameList = new SDNNameList();
                    sdnNameList.setFirstName(strFirstName);
                    sdnNameList.setMiddleName(strMiddleName);
                    sdnNameList.setLastName(strLastName);
                    sdnNameList.setFullName(strFullName);
                    sdnNameList.setRecordId(strRecordId);
                    sdnNamesLists.add(sdnNameList);
                }
                return null;
            });
            long endTime = System.currentTimeMillis();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sdnNamesLists;
    }
    private static void validateNotEmpty(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null.");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be empty.");
        }
    }


    // 1) Normalize a name (remove punctuation, convert to uppercase, etc.)
    private static String normalize(String name) {
        if (name == null) return "";

        // Remove diacritics (accents) and punctuation.
        String normalized = Normalizer.normalize(name, Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")      // Remove diacritical marks.
                .replaceAll("[^a-zA-Z0-9\\s]", "") // Remove punctuation.
                .toLowerCase()
                .trim()
                .replaceAll("\\s+", " ");
        return normalized;
    }

    // 2) Tokenize a name (split on whitespace)
    private static List<String> tokenize(String name, boolean removeStopWord) {
        // Split on one or more spaces
        String[] parts = name.split("\\s+");
        if(removeStopWord){
            List<String> tokens = Arrays.stream(parts)
                    .filter(part -> !STOPWORDS.contains(part))
                    .collect(Collectors.toList());
            if (tokens.isEmpty()) {
                tokens.add("");
            }
            return tokens;
        }else{
            return new ArrayList<>(Arrays.asList(parts));
        }
    }

    // Compute Levenshtein-based similarity ratio between 0 and 1
    private static double levenshteinRatio(String s1, String s2) {
        LevenshteinDistance lev = new LevenshteinDistance();
        int distance = lev.apply(s1, s2);
        int maxLen = Math.max(s1.length(), s2.length());
        if (maxLen == 0) return 1.0; // trivially same if both empty
        return 1.0 - (double) distance / maxLen;
    }

    // Word-level similarity calculation
    private static double wordSimilarity(String w1, String w2, boolean usePhonetic) {
        // Quick sanity check
        if (w1.isEmpty() || w2.isEmpty()) {
            return 0.0;
        }

        // Exact match
        if (w1.equals(w2)) {
            return 1.0;  // 100%
        }

        // Fallback: Levenshtein + Soundex
        double finalScore = 0.00;

        // First try Levenshtein ratio without Soundex
        double levRatio = levenshteinRatio(w1, w2);

        // If very similar, don't even need to check Soundex
        if (levRatio > 0.8) {
            return levRatio;
        }

        // Now check Soundex
        Soundex soundex = new Soundex();
        try {
            String w1Encode = soundex.encode(w1);
            String w2Encode = soundex.encode(w2);

            if (usePhonetic || w1Encode.equals(w2Encode)) {
                // Boost the score a bit for phonetic matches
                finalScore = levRatio * 1.1;
            } else {
                finalScore = levRatio;
            }
        } catch (Exception e) {
            // Soundex can fail for certain strings, fallback to levenshtein
            finalScore = levRatio;
        }

        return Math.min(finalScore, 1.0);
    }

    // Improved calculateNewBestScore that tracks used tokens
    private static MatchResult calculateNewBestScore(List<String> inputTokens, List<String> sdnNameTokens,
                                                     double positionWeightage, Set<Integer> alreadyUsedInputTokens) {
        double sum = 0.0;
        Map<Integer, Double> bestMatches = new HashMap<>();
        Map<Integer,Double> inputNameMatchedScore = new HashMap<>();
        Map<Integer,Double> sdnMatchedBestScore = new HashMap<>();
        List<NameMatchResult> nameMatchResults = new ArrayList<>();
        double sdnMatchedCounts = 0.0;

//        for (String nameToken : nameTokens)
        for (int nIndex = 0; nIndex < sdnNameTokens.size(); nIndex++) {
            double best = 0.0;
            int bestTokenIndex = -1;
            String nameToken = sdnNameTokens.get(nIndex);

            // Find best matching token that hasn't been used yet
            for (int i = 0; i < inputTokens.size(); i++) {

                NameMatchResult nameMatchResult = new NameMatchResult();
                nameMatchResult.SDNNameToken = nameToken;
                nameMatchResult.SDNTokenIndex = nIndex;

                if (alreadyUsedInputTokens.contains(i)) {
//                    System.out.printf("Input Name Token: %s already used, So skipping \n", inputTokens.get(i));
                    nameMatchResult.InputNameToken = inputTokens.get(i);
                    nameMatchResult.InputTokenIndex = i;
                    nameMatchResult.matchScore = 0;
                    nameMatchResults.add(nameMatchResult);
                    continue;  // Skip already used tokens
                }

                String inputToken = inputTokens.get(i);
                double sc = wordSimilarity(nameToken, inputToken, true);

//                System.out.printf("SDN Name Token: %s vs Input Name Token: %s => Score: %.2f \n",
//                        nameToken, inputToken, sc);

                nameMatchResult.InputNameToken = inputToken;
                nameMatchResult.InputTokenIndex = i;
                nameMatchResult.matchScore = sc;

                if (sc > BEST_SCORE_THRESHOLD) {
                    sdnMatchedCounts++;
                }
                if (sc > best && sc > BEST_SCORE_THRESHOLD) {
                    best = sc;
                    bestTokenIndex = i;
                    inputNameMatchedScore.put(i, sc);
                } else {
                    inputNameMatchedScore.put(i, 0.00);
                }

                nameMatchResults.add(nameMatchResult);
            }

            // Mark token as used if we found a good match
            if (bestTokenIndex >= 0) {
                alreadyUsedInputTokens.add(bestTokenIndex);
                bestMatches.put(bestTokenIndex, best);
                sdnMatchedBestScore.put(nIndex, best);
            }

            sum += best;
        }

        // Average the scores only if there are nameTokens
        double avgScore = sdnNameTokens.isEmpty() ? 0 : (sum / sdnNameTokens.size()) * positionWeightage;

        // If we found at least one match, ensure a minimum score based on position importance
        if (!bestMatches.isEmpty() && avgScore < MIN_COMPONENT_WEIGHT * positionWeightage) {
            avgScore = MIN_COMPONENT_WEIGHT * positionWeightage;
        }

        return new MatchResult(avgScore, bestMatches, inputNameMatchedScore, sdnMatchedBestScore, nameMatchResults , sdnMatchedCounts);
    }
    // Helper class to hold Token Name Matching
    private static class NameMatchResult {
        double matchScore;
        String InputNameToken;
        String SDNNameToken;
        int InputTokenIndex;
        int SDNTokenIndex;

        @Override
        public String toString() {
            return "NameMatchResult{InputNameToken='" + InputNameToken + "', SDNNameToken=" + SDNNameToken
                    + "', matchScore=" + matchScore +"}";
        }
    }

    // Helper class to track match results and used tokens
    private static class MatchResult {
        final double score;
        final Map<Integer, Double> tokenMatches;
        final Map<Integer, Double> nameTokenMatches;
        final Map<Integer, Double> nameTokenBestScore;
        final List<NameMatchResult> nameMatchResults;
        final double sdnMatchedCounts;

        MatchResult(double score, Map<Integer, Double> tokenMatches, Map<Integer, Double> nameTokenMatches,
                    Map<Integer, Double> nameTokenBestScore, List<NameMatchResult> nameMatchResults , double sdnMatchedCounts) {
            this.score = score;
            this.tokenMatches = tokenMatches;
            this.nameTokenMatches = nameTokenMatches;
            this.nameTokenBestScore = nameTokenBestScore;
            this.nameMatchResults = nameMatchResults;
            this.sdnMatchedCounts = sdnMatchedCounts;
        }
    }

    // Calculate coverage score - how much of each name is covered by matches
    private static double calculateCoverageScore(List<String> inputTokens, List<String> nameTokens,
                                                 Set<Integer> matchedInputIndices) {
        int inputCovered = matchedInputIndices.size();
        int totalInput = inputTokens.size();

        double inputCoverage = totalInput == 0 ? 1.0 : (double) inputCovered / totalInput;

        // More balanced approach to coverage
        if (inputCoverage >= 0.8) {
            // Great coverage - give full score
            return 1.0;
        } else if (inputCoverage >= 0.6) {
            // Good coverage - slight penalty
            return 0.95;
        } else {
            // Poor coverage - stronger penalty
            return 0.85;
        }
    }

    // Check for repeating elements and apply penalty if needed
    private static double calculateRepeatingElementPenalty(List<String> tokens) {
        Set<String> uniqueTokens = new HashSet<>(tokens);

        // Count repeating elements
        int duplicateCount = tokens.size() - uniqueTokens.size();

        if (duplicateCount > 0) {
            // Apply stronger penalty for names that are mostly duplicates
            if (duplicateCount >= tokens.size() / 2) {
                return REPEATING_ELEMENT_PENALTY * 0.9;
            }
            return REPEATING_ELEMENT_PENALTY;
        }

        return 1.0;  // No penalty
    }

    // Get unmatched token count
    private static int getUnMatchedTokens(List<String> inputTokens, Set<Integer> matchedInputIndices) {
        return inputTokens.size() - matchedInputIndices.size();
    }

    // Check if first and last names match
    private static boolean compareFirstAndLast(List<String> nameTokens, SDNNameList sdnTokens) {
        // Check if both strings have at least a first name
        if (nameTokens.isEmpty() || sdnTokens.firstName().isEmpty()) {
            return false;
        }

        // Extract the first and last names
        String nameFirst = nameTokens.get(0);
        String nameLast = nameTokens.size() > 1 ? nameTokens.get(nameTokens.size() - 1) : "";
        String sdnFirst = sdnTokens.firstName();
        String sdnLast = sdnTokens.lastName();

        // Taking FirstName Similarity
        double firstNameSimilarity = levenshteinRatio(nameFirst, sdnFirst);

        // Taking LastName Similarity
        double lastNameSimilarity = levenshteinRatio(nameLast, sdnLast);

        return (firstNameSimilarity >= UNMATCHED_TOKEN_SIMILARITY && lastNameSimilarity >= UNMATCHED_TOKEN_SIMILARITY);
    }

    private static double calculateStructuralScore(List<String> tokensInputName,
                                                   SDNNameList sdnName, List<String> tokenSDNName) {
        double structuralScore = 1.00;

        // Less penalty for complex names with different token counts
        if (tokensInputName.size() != tokenSDNName.size()) {
            if (tokensInputName.size() >= 4 && tokenSDNName.size() >= 4) {
                // For complex names, be more lenient about token count
                structuralScore *= 0.98;
            } else {
                structuralScore *= PENALTY_WEIGHT;
            }
        }

        // Check if First Name and Last Name Swapped
        if (!compareFirstAndLast(tokensInputName, sdnName)) {
            structuralScore *= PENALTY_WEIGHT;
        }

        return structuralScore;
    }

    private static boolean checkTokenEmpty(List<String> tokens) {
        return tokens == null || tokens.isEmpty() ||
                tokens.stream().allMatch(str -> str == null || str.trim().isEmpty());
    }

    // Calculate initial phonetic score between tokens
    private static double calculateScore(String w1, String w2) {
        // If exact match, return 100%
        if (w1.equals(w2)) {
            return 100.0;
        }

        // Try Soundex comparison
        Soundex soundex = new Soundex();
        try {
            String w1Encode = soundex.encode(w1);
            String w2Encode = soundex.encode(w2);
            if (w1Encode.equals(w2Encode)) {
                double score = levenshteinRatio(w1, w2) * 100.0;
                return Math.round(score);
            }
        } catch (Exception e) {
            // Handle potential exceptions with Soundex
        }

        // If no phonetic match, check if still very similar
        double levScore = levenshteinRatio(w1, w2);
        if (levScore > 0.85) {
            return Math.round(levScore * 90.0); // Slightly lower than phonetic match
        }

        return 0.00;
    }

    private static boolean determinePenaltyNoMatchingSDN(MatchResult nameMatchResult) {

        long zeroCount = nameMatchResult.nameTokenMatches.entrySet().stream()
                .filter(e -> e.getValue() != null && e.getValue() == 0.0)
                .count();
        long totalTokenCount = nameMatchResult.nameTokenMatches.size();
        return zeroCount == totalTokenCount;
    }

    private static boolean determineStopwordsRemove(String normInputName, String normSDNFullName) {

        boolean foundINInputName = STOPWORDS.stream().anyMatch(normInputName::contains);
        boolean foundINSDNName = STOPWORDS.stream().anyMatch(normSDNFullName::contains);

        if(foundINInputName && foundINSDNName){
            return false;
        }else{
            return true;
        }

//        return foundINInputName && foundINSDNName;
    }

    private static double recalculateScore(List<String> inputToken, MatchResult matchResult , double positionWeightage){

        if(matchResult.nameMatchResults.size()>inputToken.size()){
            double best = 0.0;
            double  tokens = 0;
            boolean hasFullMatchCalculated = false;
            for(NameMatchResult nameMatchResult : matchResult.nameMatchResults){

                if(nameMatchResult.matchScore > BEST_SCORE_THRESHOLD) {
                    if (!hasFullMatchCalculated) {
                        tokens++;
                        hasFullMatchCalculated = true;
                    }
                }

                if (nameMatchResult.matchScore > best && nameMatchResult.matchScore > BEST_SCORE_THRESHOLD) {
                    best = nameMatchResult.matchScore;
                }
            }
            return (best/tokens) * positionWeightage;
        }else{
            return matchResult.score;
        }
    }

    // Doing Monkey Fix
    private static double calculateRedefinedScore(List<String> inputToken,MatchResult firstNameMatchResult,
                                                  MatchResult middleNameMatchResult, MatchResult lastNameMatchResult
    ){
        // First calculate differance in SDN and Input Name
//        double firstNameScore = firstNameMatchResult.score;
//        double middleNameScore = middleNameMatchResult.score;
//        double lastNameScore = lastNameMatchResult.score;

        double firstNameScore = recalculateScore(inputToken, firstNameMatchResult , FIRSTNAME_WEIGHT);
        double middleNameScore = recalculateScore(inputToken, middleNameMatchResult , MIDDLENAME_WEIGHT);
        double lastNameScore = recalculateScore(inputToken, lastNameMatchResult, LASTNAME_WEIGHT);

        // Final combined score (weighted average)
        double finalScore = ((firstNameScore) + (middleNameScore) + (lastNameScore));


        double denominator = ((firstNameMatchResult.nameTokenBestScore.isEmpty() ? 0.0 : FIRSTNAME_WEIGHT)
                + (middleNameMatchResult.nameTokenBestScore.isEmpty() ? 0.0 : MIDDLENAME_WEIGHT)
                + (lastNameMatchResult.nameTokenBestScore.isEmpty() ? 0.0 : LASTNAME_WEIGHT));


        finalScore = finalScore / denominator;

        // Need to apply PENALTY for no matching in SDN Name
        final double PENALTY_VALUE = 0.9;

        for(int index=0;index<inputToken.size();index++) {
            if(firstNameMatchResult.nameTokenMatches.get(index).equals(0.0) &&
                    middleNameMatchResult.nameTokenMatches.get(index).equals(0.0) &&
                    lastNameMatchResult.nameTokenMatches.get(index).equals(0.0) ){
                finalScore = finalScore * PENALTY_VALUE;
            }
        }

        return finalScore;
    }



    // Revised name similarity check
    public static double nameSimilarityCheck(String inputName, SDNNameList sdnName) {

        double akaScore = handleAKANames(inputName, sdnName);
        if (akaScore >= 0) {
            return akaScore;
        }
        double finalScore = 0.00;

        // --- Normalize ---
        String normInput = normalize(inputName);
        String normFirstName = normalize(sdnName.firstName());
        String normMiddleName = normalize(sdnName.middleName());
        String normLastName = normalize(sdnName.lastName());
        String normFullName = normalize(sdnName.fullName());

        SDNNameList normSDNName = new SDNNameList(normFirstName, normLastName, normMiddleName, normFullName);

        // If either is empty, return 0
        if (normInput.isEmpty() || normFullName.isEmpty()) {
            return 0.0;
        }

        // If both exact match, return 100%
        if (normInput.equalsIgnoreCase(normFullName)) {
            return 100.0;
        }

        // Determine remove stopwords If both input and sdn name have any one stopwrds
        boolean removeStopWord =  determineStopwordsRemove(normInput, normFullName);

        // --- Tokenize ---
        // For Input Name we shouldn't remove stop-words
        List<String> tokensInput = tokenize(normInput, false);

        List<String> tokenSDNName = tokenize(normFullName, removeStopWord);
        List<String> tokensFirstName = tokenize(normFirstName, removeStopWord);
        List<String> tokensMiddleName = tokenize(normMiddleName, removeStopWord);
        List<String> tokensLastName = tokenize(normLastName, removeStopWord);

        boolean isFirstNameEmpty = checkTokenEmpty(tokensFirstName);
        boolean isMiddleNameEmpty = checkTokenEmpty(tokensMiddleName);
        boolean isLastNameEmpty = checkTokenEmpty(tokensLastName);

        if (tokensInput.isEmpty() || tokenSDNName.isEmpty()) {
            return 0.0;
        }

        // Quick check for potential similarity before doing detailed analysis
        String inputNameFirstToken = tokensInput.get(0);
        String inputNameLastToken = tokensInput.get(tokensInput.size() - 1);
        String sdnNameFirstToken = tokenSDNName.get(0);
        String sdnNameLastToken = tokenSDNName.get(tokenSDNName.size() - 1);

        double firstTokenSimilarity = wordSimilarity(inputNameFirstToken, sdnNameFirstToken, true);
        double lastTokenSimilarity = wordSimilarity(inputNameLastToken, sdnNameLastToken, true);

        // If either first or last tokens are similar, proceed with detailed analysis
        //if (firstTokenSimilarity >= 0.6 || lastTokenSimilarity >= 0.6)
        {
            // Track which input tokens have been used
            Set<Integer> usedInputTokens = new HashSet<>();
            Map<Integer, Double> allMatches = new HashMap<>();

            // Process each name component, tracking which tokens are used
//            System.out.printf("SDN FirstName:%s vs. InputName:%s\n", String.join(" ", tokensFirstName),
//                    String.join(" ", tokensInput));
            MatchResult firstNameResult = calculateNewBestScore(tokensInput, tokensFirstName,
                    FIRSTNAME_WEIGHT, usedInputTokens);
//            System.out.printf("\nSDN MiddleName:%s vs. InputName:%s\n", String.join(" ", tokensMiddleName),
//                    String.join(" ", tokensInput));
            MatchResult middleNameResult = calculateNewBestScore(tokensInput, tokensMiddleName,
                    MIDDLENAME_WEIGHT, usedInputTokens);
//            System.out.printf("\nSDN LastName:%s vs. InputName:%s\n", String.join(" ", tokensLastName),
//                    String.join(" ", tokensInput));
            MatchResult lastNameResult = calculateNewBestScore(tokensInput, tokensLastName,
                    LASTNAME_WEIGHT, usedInputTokens);

            // Collect all matches for coverage calculation
            allMatches.putAll(firstNameResult.tokenMatches);
            allMatches.putAll(middleNameResult.tokenMatches);
            allMatches.putAll(lastNameResult.tokenMatches);

            // Component scores
            double firstNameScore = firstNameResult.score;
            double middleNameScore = middleNameResult.score;
            double lastNameScore = lastNameResult.score;

            // Final combined score (weighted average)
            finalScore = ((firstNameScore) + (middleNameScore) + (lastNameScore));
            double denominator = ((isFirstNameEmpty ? 0.0 : FIRSTNAME_WEIGHT)
                    + (isMiddleNameEmpty ? 0.0 : MIDDLENAME_WEIGHT)
                    + (isLastNameEmpty ? 0.0 : LASTNAME_WEIGHT));
            finalScore = finalScore / denominator;

            // Redefined Best Score Calculation - Monkey Fix
            if ((tokensInput.size() < tokenSDNName.size()) && (tokenSDNName.size()-tokensInput.size()==1)) {
                // Call Redefined Score calculation
                finalScore = calculateRedefinedScore(tokensInput, firstNameResult, middleNameResult, lastNameResult);
            }

            // Complex name bonus
            if (tokensInput.size() >= 4 && tokenSDNName.size() >= 4 && usedInputTokens.size() >= 3 &&
                    finalScore <= 0.95) {
                finalScore *= COMPLEX_NAME_BONUS;
            }

            // Calculate coverage score
            double coverageScore = calculateCoverageScore(tokensInput, tokenSDNName, usedInputTokens);

            // Repeating element check - reduced impact
            double repeatingPenalty = 1.0;
            if (tokenSDNName.size() - new HashSet<>(tokenSDNName).size() > 1) {
                // Only apply significant penalty for multiple repeats
                repeatingPenalty = REPEATING_ELEMENT_PENALTY;
            }

            // Applying UnMatched Tokens Penalty - with a reduced impact
            int unMatchedTokens = getUnMatchedTokens(tokensInput, usedInputTokens);
            double penaltyValue = 0;

            // Only apply unmatched token penalty if a significant portion is unmatched
            if (unMatchedTokens > tokensInput.size() / 3) {
                penaltyValue = (unMatchedTokens * (1 - PENALTY_WEIGHT) * 0.5);
            }

            finalScore = finalScore * (1 - penaltyValue) * coverageScore * repeatingPenalty;

            // Applying Structural Penalty
            double structuralScore = calculateStructuralScore(tokensInput, normSDNName, tokenSDNName);

            finalScore = finalScore * structuralScore;

            // Higher floor for scores with strong first/last match
            if ((firstTokenSimilarity > 0.8 && lastTokenSimilarity > 0.7) && finalScore < 0.7) {
                finalScore = 0.7;
            }


            boolean skipRepeatingTokenPenalty = false;
            long repeatedCount = 0;

            if (finalScore <= 0.5 && !normLastName.isEmpty() && tokensLastName.size() <= 1 && lastNameScore <= 0
                    && firstNameScore > 0 && middleNameScore > 0) {
                skipRepeatingTokenPenalty = true;

                double totalNameMatchScore = ((firstNameScore) + (middleNameScore) + (lastNameScore));
                double totalNameMatchScoreDenominator = ((firstNameScore > 0.0 ? FIRSTNAME_WEIGHT : 0.0)
                        + (middleNameScore > 0.0 ? MIDDLENAME_WEIGHT : 0.0) + (lastNameScore > 0.0 ? LASTNAME_WEIGHT
                        : firstNameScore >= 0.60 && middleNameScore >= 0.40 ? LASTNAME_DENOMINATOR : 0.0));

                finalScore = totalNameMatchScore / totalNameMatchScoreDenominator;

                repeatedCount = tokenSDNName.stream()
                        .collect(Collectors.groupingBy(word -> word.toLowerCase(), Collectors.counting())).values()
                        .stream().filter(count -> count > 1).count();

                double penalties = unMatchedTokens * INPUT_PENALTY_WEIGHT;
                finalScore = finalScore * (1 - penalties);

            }

            double unusedTokens = tokenSDNName.size() - (firstNameResult.sdnMatchedCounts
                    + middleNameResult.sdnMatchedCounts + lastNameResult.sdnMatchedCounts);
            double sdnUnusedTokens = tokenSDNName.size() - allMatches.size();

            if (sdnUnusedTokens > 0 && skipRepeatingTokenPenalty) {
                boolean isRepeatingPenalty = repeatedCount > 0 && sdnUnusedTokens >= repeatedCount ? true : false;
                sdnUnusedTokens = sdnUnusedTokens - repeatedCount;
                double penalties = sdnUnusedTokens * SDN_PENALTIES_WEIGHT;
                finalScore = finalScore * (1 - penalties);
                if (isRepeatingPenalty) {
                    penalties = repeatedCount * SDN_REPEATING_PENALTY_WEIGHT;
                    finalScore = finalScore * (1 - penalties);
                }

            } else if (tokensInput.size() < tokenSDNName.size()) {

                double penalties = unusedTokens * SDN_PENALTY_WEIGHT;
                finalScore = finalScore * (1 - penalties);
            }

            // Convert to percentage, round to two decimals
            finalScore = Math.round(finalScore * 10000.0) / 100.0;
        }

        return finalScore;
    }

    public static ArrayList<SDNNameList> processNameMatch(String inputName,
                                                          List<SDNNameList> sdnNames, double threshold) {
        ArrayList<SDNNameList> matchedRecords = new ArrayList<>();
        for (SDNNameList sdn : sdnNames) {
            double score = nameSimilarityCheck(inputName, sdn);
            sdn.setScore(score);
            if(score > threshold) {
                Optional<SDNNameList> existingRecord = matchedRecords.stream()
                        .filter(r -> r.recordId().equals(sdn.recordId()))
                        .findFirst();
                if(existingRecord.isPresent() && sdn.Score() > existingRecord.get().Score()) {
                    // removeIf uses a lambda expression to remove any record with the matching id.
                    matchedRecords.removeIf(r -> r.recordId().equals(existingRecord.get().recordId()));
                    matchedRecords.add(sdn);
                } else if(!existingRecord.isPresent()){
                    matchedRecords.add(sdn);
                }
            }
        }
        return matchedRecords;
    }
    private static void DoNameSimilarityAgainstSDNDB(String inputName, double threshold,String outputDir){

        List<SDNNameList> sdnNames = new ArrayList<>();

        long startTime = System.nanoTime(); // Start time

        sdnNames.addAll(pullNamesFromDatabase(inputName,"Person","WATCH_LIST",threshold));
        sdnNames.addAll(pullNamesFromDatabase(inputName,"Person","STATE_OWNED",threshold));

        long endTime = System.nanoTime(); // End time
        long duration = (endTime - startTime) / 1_000_000; // Duration in milliseconds

        System.out.println("Fetching time: " + duration + " ms" + "");

        //Process Name Match input Name vs SDN Names
        startTime = System.nanoTime(); // Start time

        ArrayList<SDNNameList> matchedRecords = processNameMatch(inputName, (List<SDNNameList>) sdnNames, threshold);

        for(SDNNameList sdn : matchedRecords){
//            System.out.printf("Input: %s  vs.  SDN: FirstName: %s MiddleName: %s LastName: %s  =>  %.2f%% %n",
//                    inputName, sdn.firstName(), sdn.middleName(), sdn.lastName(), sdn.Score());
            System.out.printf("Input: %s  vs.  SDN: %s  Record Id: %s =>  %.2f%% %n",
                    inputName, sdn.fullName(), sdn.recordId(), sdn.Score());
        }

        System.out.println("Total Matched Records: " + matchedRecords.size());
        writeResultsToExcel(inputName, matchedRecords, outputDir);


        endTime = System.nanoTime(); // End time
        duration = (endTime - startTime) / 1_000_000; // Duration in milliseconds
        System.out.println("Execution time: " + duration + " ms");

    }

    private static void writeResultsToExcel(String inputName, List<SDNNameList> matchedRecords, String filePath) {
        XSSFWorkbook workbook = null;
        FileOutputStream fos = null;

        try {
            File file = new File(filePath);

            if (file.exists()) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    workbook = new XSSFWorkbook(fis);
                }
            } else {
                workbook = new XSSFWorkbook();
            }
            inputName = inputName.replaceAll("[\\\\/?*\\[\\]:]", "_");
            Sheet sheet = workbook.getSheet(inputName);
            if (sheet != null) {
                int index = workbook.getSheetIndex(sheet);
                workbook.removeSheetAt(index);
            }

            sheet = workbook.createSheet(inputName);

            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("Input Name");
            header.createCell(1).setCellValue("Matched Full Name - NMP");
            header.createCell(2).setCellValue("Record ID - NMP");

            int rowNum = 1;
            for (SDNNameList sdn : matchedRecords) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(inputName);
                row.createCell(1).setCellValue(sdn.fullName());
                row.createCell(2).setCellValue(sdn.recordId());
            }

            for (int i = 0; i < 3; i++) {
                sheet.autoSizeColumn(i);
            }

            fos = new FileOutputStream(filePath);
            workbook.write(fos);

            System.out.println("Results written to Excel: " + filePath);

        } catch (IOException e) {
            throw new RuntimeException("Failed to write Excel", e);
        } finally {
            try {
                if (fos != null)
                    fos.close();
                if (workbook != null)
                    workbook.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    public static class SDNNameList {
        private String firstName;
        private String lastName;
        private String middleName;
        private String fullName;
        private String recordId;
        private double score;

        public SDNNameList() {
            this.firstName = "";
            this.lastName = "";
            this.middleName = "";
            this.fullName = "";
            this.recordId = "";
            this.score = 0.0;
        }

        public SDNNameList(String firstName, String lastName, String middleName, String fullName) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.middleName = middleName;
            this.fullName = fullName;
            this.recordId = "";
            this.score = 0.0;
        }

        public String firstName() { return firstName; }
        public String lastName() { return lastName; }
        public String middleName() { return middleName; }
        public String fullName() { return fullName; }
        public String recordId() { return recordId; }
        public double Score() { return score; }

        public void setFirstName(String firstName) { this.firstName = firstName; }
        public void setLastName(String lastName) { this.lastName = lastName; }
        public void setMiddleName(String middleName) { this.middleName = middleName; }
        public void setFullName(String fullName) { this.fullName = fullName; }
        public void setRecordId(String recordId) { this.recordId = recordId; }
        public void setScore(double score) { this.score = score; }
    }

    public static List<String> getMatchingSdnNames(String inputName, String type, String fileType, String nmp, String auditId) {

        // List to store matching SDN names
        long startMatchingTime = System.currentTimeMillis();
        double nameMatchPercentage = Double.parseDouble(nmp);
        List<SDNNameList> sdnNameLists = pullNamesFromDatabase(inputName, type, fileType, nameMatchPercentage);
        long endMatchingTime = System.currentTimeMillis();


        // Start time for performance tracking
        long startTime = System.nanoTime();
        // Process each SDN name and calculate similarity
        startMatchingTime = System.currentTimeMillis();
        ArrayList<SDNNameList> sdnNameLists1 = processNameMatch(inputName, sdnNameLists, nameMatchPercentage);
        endMatchingTime = System.currentTimeMillis();


        return sdnNameLists1.stream()
                .map(SDNNameList::recordId)
                .filter(Objects::nonNull).distinct().collect(Collectors.toList());
    }
    private static double handleAKANames(String inputName, SDNNameList sdnName) {
        if (inputName == null || !inputName.contains("@")) {
            return -1.0; // Signal to proceed with normal flow
        }

        // Direct match with full name (before splitting)
        if (inputName.equalsIgnoreCase(sdnName.fullName())) {
            return 100.0;
        }
        String[] nameParts = inputName.split("@");

        nameParts[0] = String.join(" ",tokenize(nameParts[0], true));
        nameParts[1] = String.join(" ",tokenize(nameParts[1], true));

        double wordSimilarity = wordSimilarity(nameParts[0],nameParts[1],true);
        if(wordSimilarity >= 0.6){
            String firstWord = nameParts[0];
            // Recursive call
            return nameSimilarityCheck(firstWord, sdnName) - 5.0;
        } else {

            String partOne[] = nameParts[0].trim().replaceAll("//s", " ").split(" ");
            String partTwo[] = nameParts[1].trim().replaceAll("//s", " ").split(" ");

            Arrays.sort(partOne);
            Arrays.sort(partTwo);

            String sortedPartOne = String.join(" ", partOne);
            String sortedPartTwo = String.join(" ", partTwo);

            double score = wordSimilarity(sortedPartOne , sortedPartTwo , true);

            if(score  >= 0.6) {

                String firstWord = nameParts[0];
                return nameSimilarityCheck(firstWord, sdnName) - 5.0;
            }


            // Also calculate score using the full unmodified name
            inputName = normalize(inputName);
            return nameSimilarityCheck(inputName, sdnName);
        }
    }

}
