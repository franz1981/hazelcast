/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster.memberselector;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;

/**
 * Selects a member if one of the sub-selectors succeed
 */
class OrMemberSelector implements MemberSelector {

    private final MemberSelector[] selectors;

    public OrMemberSelector(MemberSelector... selectors) {
        this.selectors = selectors;
    }

    @Override
    public boolean select(Member member) {
        for (MemberSelector selector : selectors) {
            if (selector.select(member)) {
                return true;
            }
        }

        return false;
    }
}