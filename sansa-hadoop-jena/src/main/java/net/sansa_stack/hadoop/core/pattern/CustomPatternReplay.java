package net.sansa_stack.hadoop.core.pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A caching pattern whose matchers can be replay prior matches */
public class CustomPatternReplay
    extends CustomPatternDecoratorBase
{
    protected CustomPatternReplay(CustomPattern decoratee) {
        super(decoratee);
    }

    public static CustomPatternReplay wrap(CustomPattern decoratee) {
        return new CustomPatternReplay(decoratee);
    }

    @Override
    public CustomMatcherReplay matcher(CharSequence charSequence) {
        CustomMatcher matcher = pattern.matcher(charSequence);
        return new CustomMatcherReplay(matcher);
    }

    public static class CustomMatcherReplay
        extends CustomMatcherDecorator
    {
        protected CustomMatcher decoratee;

        protected List<Match> matches = new ArrayList<>();

        protected int matchId;
        protected boolean isFinished = false;

        public CustomMatcherReplay(CustomMatcher decoratee) {
            super();
            this.decoratee = decoratee;
            reset();
        }

        public void reset() {
            matchId = -1;
        }

        @Override
        protected CustomMatcher getDecoratee() {
            return decoratee;
        }

        @Override
        public boolean find() {
            boolean result = false;
            if (matchId + 1 < matches.size()) {
                ++matchId;
                result = true;
            } else if (!isFinished) {
                result = super.find();
                if (result) {
                    // Cache the found state
                    int n = decoratee.groupCount();
                    Region[] regions = new Region[n];
                    for (int i = 0; i < n; ++i) {
                        regions[i] = new Region(decoratee.start(i), decoratee.end(i));
                    }

                    Match match = new Match();
                    match.groups = regions;
                    matches.add(match);
                    ++matchId;
                } else {
                    isFinished = true;
                }
            }

            return result;
        }

        @Override
        public int start() {
            return start(0);
        }

        @Override
        public int end() {
            return end(0);
        }

        @Override
        public int start(int group) {
            return matches.get(matchId).groups[group].start;
        }

        @Override
        public int end(int group) {
            return matches.get(matchId).groups[group].end;
        }

        @Override
        public int groupCount() {
            return matches.get(matchId).groups.length;
        }

        @Override
        public void region(int start, int end) {
            throw new UnsupportedOperationException();
        }

        // Calling this method is only valid if the current matchId refers to the decoratee.
        private Region getOrCacheRegion(String name) {
            Region result = matches.get(matchId).namedGroups().computeIfAbsent(name, k -> {
                if (matchId + 1 != matches.size()) {
                    throw new IllegalStateException("Replayed match has no cached entry for group with name " + name);
                }
                return new Region(decoratee.start(name), decoratee.end(name));
            });
            // System.out.println("Region: " + name + " -> " + result);
            return result;
        }

        @Override
        public int start(String name) {
            return getOrCacheRegion(name).start;
        }

        @Override
        public int end(String name) {
            return getOrCacheRegion(name).end;
        }
    }

    public static class Match {
        protected Region[] groups;
        protected Map<String, Region> namedGroups = null;

        public Map<String, Region> namedGroups() {
            if (namedGroups == null) {
                namedGroups = new HashMap<>();
            }
            return namedGroups;
        }


//        public void put(String name, int start, int end) {
//            if (namedGroups == null) {
//                namedGroups = new HashMap<>();
//            }
//
//            namedGroups.put(name, new Region(start, end));
//        }
    }

    public static class Region {
        public Region(int start, int end) {
            super();
            this.start = start;
            this.end = end;
        }
        int start;
        int end;

        @Override
        public String toString() {
            return "Region [start=" + start + ", end=" + end + "]";
        }
    }
}
