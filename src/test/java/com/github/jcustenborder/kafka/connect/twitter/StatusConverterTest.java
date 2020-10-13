/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.twitter;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.User;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatusConverterTest {

    public static GeoLocation mockGeoLocation() {
        return new GeoLocation(30.2672D, 97.7431D);
    }

    public static Place mockPlace() {
        Place place = mock(Place.class);
        when(place.getName()).thenReturn("Example place");
        when(place.getStreetAddress()).thenReturn("123 Example St");
        when(place.getCountryCode()).thenReturn("US");
        when(place.getId()).thenReturn("asdfaisdfasd");
        when(place.getCountry()).thenReturn("United States");
        when(place.getPlaceType()).thenReturn("ADF");
        when(place.getURL()).thenReturn("http://www.example.com/");
        when(place.getFullName()).thenReturn("Example place");
        return place;
    }

    public static Status mockStatus() {
        Status status = mock(Status.class);
        User user = mockUser();
        GeoLocation geoLocation = mockGeoLocation();
        Place place = mockPlace();

        when(status.getCreatedAt()).thenReturn(new Date(1471667709998L));
        when(status.getId()).thenReturn(9823452L);
        when(status.getText()).thenReturn("This is a twit");
        when(status.getSource()).thenReturn("foo");
        when(status.isTruncated()).thenReturn(false);
        when(status.getInReplyToStatusId()).thenReturn(2345234L);
        when(status.getInReplyToUserId()).thenReturn(8756786L);
        when(status.getInReplyToScreenName()).thenReturn("foo");
        when(status.getGeoLocation()).thenReturn(geoLocation);
        when(status.getPlace()).thenReturn(place);
        when(status.isFavorited()).thenReturn(true);
        when(status.isRetweeted()).thenReturn(false);
        when(status.getFavoriteCount()).thenReturn(1234);
        when(status.getUser()).thenReturn(user);
        when(status.isRetweet()).thenReturn(false);
        when(status.getContributors()).thenReturn(new long[]{431234L, 986789678L});
        when(status.getRetweetCount()).thenReturn(1234);
        when(status.isRetweetedByMe()).thenReturn(false);
        when(status.getCurrentUserRetweetId()).thenReturn(653456345L);
        when(status.isPossiblySensitive()).thenReturn(false);
        when(status.getLang()).thenReturn("en-US");
        when(status.getWithheldInCountries()).thenReturn(new String[]{"CN"});

        return status;
    }

    public static User mockUser() {
        User user = mock(User.class);

        when(user.getId()).thenReturn(1234L);
        when(user.getName()).thenReturn("Example User");
        when(user.getScreenName()).thenReturn("example");
        when(user.getLocation()).thenReturn("Austin, TX");
        when(user.getDescription()).thenReturn("This is a description");
        when(user.isContributorsEnabled()).thenReturn(true);
        when(user.getProfileImageURL()).thenReturn("http://i.twittercdn.com/profile.jpg");
        when(user.getBiggerProfileImageURL()).thenReturn("http://i.twittercdn.com/biggerprofile.jpg");
        when(user.getMiniProfileImageURL()).thenReturn("http://i.twittercdn.com/mini.profile.jpg");
        when(user.getOriginalProfileImageURL()).thenReturn("http://i.twittercdn.com/original.profile.jpg");
        when(user.getProfileImageURLHttps()).thenReturn("https://i.twittercdn.com/profile.jpg");
        when(user.getBiggerProfileImageURLHttps()).thenReturn("https://i.twittercdn.com/bigger.profile.jpg");
        when(user.getMiniProfileImageURLHttps()).thenReturn("https://i.twittercdn.com/mini.profile.jpg");
        when(user.getOriginalProfileImageURLHttps()).thenReturn("https://i.twittercdn.com/original.profile.jpg");
        when(user.isDefaultProfileImage()).thenReturn(true);
        when(user.getURL()).thenReturn("https://www.twitter.com/example");
        when(user.isProtected()).thenReturn(false);
        when(user.getFollowersCount()).thenReturn(54245);
        when(user.getProfileBackgroundColor()).thenReturn("#ffffff");
        when(user.getProfileTextColor()).thenReturn("#000000");
        when(user.getProfileLinkColor()).thenReturn("#aaaaaa");
        when(user.getProfileSidebarFillColor()).thenReturn("#333333");
        when(user.getProfileSidebarBorderColor()).thenReturn("#555555");
        when(user.isProfileUseBackgroundImage()).thenReturn(true);
        when(user.isDefaultProfile()).thenReturn(true);
        when(user.isShowAllInlineMedia()).thenReturn(true);
        when(user.getFriendsCount()).thenReturn(452345234);
        when(user.getCreatedAt()).thenReturn(new Date(1471665653209L));
        when(user.getFavouritesCount()).thenReturn(12341);
        when(user.getUtcOffset()).thenReturn(8);
        when(user.getTimeZone()).thenReturn("UTC");
        when(user.getProfileBackgroundImageURL()).thenReturn("https://i.twittercdn.com/original.background.jpg");
        when(user.getProfileBackgroundImageUrlHttps()).thenReturn("https://i.twittercdn.com/original.background.jpg");
        when(user.getProfileBannerURL()).thenReturn("https://i.twittercdn.com/original.banner.jpg");
        when(user.getProfileBannerRetinaURL()).thenReturn("https://i.twittercdn.com/original.banner.jpg");
        when(user.getProfileBannerIPadURL()).thenReturn("https://i.twittercdn.com/original.banner.jpg");
        when(user.getProfileBannerIPadRetinaURL()).thenReturn("https://i.twittercdn.com/original.banner.jpg");
        when(user.getProfileBannerMobileURL()).thenReturn("https://i.twittercdn.com/original.banner.jpg");
        when(user.getProfileBannerMobileRetinaURL()).thenReturn("https://i.twittercdn.com/original.banner.jpg");
        when(user.isProfileBackgroundTiled()).thenReturn(false);
        when(user.getLang()).thenReturn("en-us");
        when(user.getStatusesCount()).thenReturn(543);
        when(user.isGeoEnabled()).thenReturn(true);
        when(user.isVerified()).thenReturn(true);
        when(user.isTranslator()).thenReturn(false);
        when(user.getListedCount()).thenReturn(4);
        when(user.isFollowRequestSent()).thenReturn(false);
        when(user.getWithheldInCountries()).thenReturn(new String[]{"CN"});


        return user;
    }

    public static StatusDeletionNotice mockStatusDeletionNotice() {
        StatusDeletionNotice statusDeletionNotice = mock(StatusDeletionNotice.class);
        when(statusDeletionNotice.getStatusId()).thenReturn(1234565345L);
        when(statusDeletionNotice.getUserId()).thenReturn(6543456354L);
        return statusDeletionNotice;
    }

    List<Long> convert(long[] values) {
        List<Long> list = new ArrayList<>();
        for (Long l : values) {
            list.add(l);
        }
        return list;
    }

    List<String> convert(String[] values) {
        List<String> list = new ArrayList<>();
        for (String l : values) {
            list.add(l);
        }
        return list;
    }

    void assertStatus(Status status, Struct struct) {
        assertEquals(status.getCreatedAt(), struct.get("CreatedAt"), "CreatedAt does not match.");
        assertEquals(status.getId(), struct.get("Id"), "Id does not match.");
        assertEquals(status.getText(), struct.get("Text"), "Text does not match.");
        assertEquals(status.getSource(), struct.get("Source"), "Source does not match.");
        assertEquals(status.isTruncated(), struct.get("Truncated"), "Truncated does not match.");
        assertEquals(status.getInReplyToStatusId(), struct.get("InReplyToStatusId"), "InReplyToStatusId does not match.");
        assertEquals(status.getInReplyToUserId(), struct.get("InReplyToUserId"), "InReplyToUserId does not match.");
        assertEquals(status.getInReplyToScreenName(), struct.get("InReplyToScreenName"), "InReplyToScreenName does not match.");
        assertEquals(status.isFavorited(), struct.get("Favorited"), "Favorited does not match.");
        assertEquals(status.isRetweeted(), struct.get("Retweeted"), "Retweeted does not match.");
        assertEquals(status.getFavoriteCount(), struct.get("FavoriteCount"), "FavoriteCount does not match.");
        assertEquals(status.isRetweet(), struct.get("Retweet"), "Retweet does not match.");
        assertEquals(status.getRetweetCount(), struct.get("RetweetCount"), "RetweetCount does not match.");
        assertEquals(status.isRetweetedByMe(), struct.get("RetweetedByMe"), "RetweetedByMe does not match.");
        assertEquals(status.getCurrentUserRetweetId(), struct.get("CurrentUserRetweetId"), "CurrentUserRetweetId does not match.");
        assertEquals(status.isPossiblySensitive(), struct.get("PossiblySensitive"), "PossiblySensitive does not match.");
        assertEquals(status.getLang(), struct.get("Lang"), "Lang does not match.");

        assertNotNull(struct, "struct should not be null.");
        assertEquals(status.getUser().getId(), struct.get("User_Id"), "Id does not match.");
        assertEquals(status.getUser().getName(), struct.get("User_Name"), "Name does not match.");
        assertEquals(status.getUser().getScreenName(), struct.get("User_ScreenName"), "ScreenName does not match.");
        assertEquals(status.getUser().getLocation(), struct.get("User_Location"), "Location does not match.");
        assertEquals(status.getUser().getDescription(), struct.get("User_Description"), "Description does not match.");
        assertEquals(status.getUser().isContributorsEnabled(), struct.get("User_ContributorsEnabled"), "ContributorsEnabled does not match.");
        assertEquals(status.getUser().getProfileImageURL(), struct.get("User_ProfileImageURL"), "ProfileImageURL does not match.");
        assertEquals(status.getUser().getBiggerProfileImageURL(), struct.get("User_BiggerProfileImageURL"), "BiggerProfileImageURL does not match.");
        assertEquals(status.getUser().getMiniProfileImageURL(), struct.get("User_MiniProfileImageURL"), "MiniProfileImageURL does not match.");
        assertEquals(status.getUser().getOriginalProfileImageURL(), struct.get("User_OriginalProfileImageURL"), "OriginalProfileImageURL does not match.");
        assertEquals(status.getUser().getProfileImageURLHttps(), struct.get("User_ProfileImageURLHttps"), "ProfileImageURLHttps does not match.");
        assertEquals(status.getUser().getBiggerProfileImageURLHttps(), struct.get("User_BiggerProfileImageURLHttps"), "BiggerProfileImageURLHttps does not match.");
        assertEquals(status.getUser().getMiniProfileImageURLHttps(), struct.get("User_MiniProfileImageURLHttps"), "MiniProfileImageURLHttps does not match.");
        assertEquals(status.getUser().getOriginalProfileImageURLHttps(), struct.get("User_OriginalProfileImageURLHttps"), "OriginalProfileImageURLHttps does not match.");
        assertEquals(status.getUser().isDefaultProfileImage(), struct.get("User_DefaultProfileImage"), "DefaultProfileImage does not match.");
        assertEquals(status.getUser().getURL(), struct.get("User_URL"), "URL does not match.");
        assertEquals(status.getUser().isProtected(), struct.get("User_Protected"), "Protected does not match.");
        assertEquals(status.getUser().getFollowersCount(), struct.get("User_FollowersCount"), "FollowersCount does not match.");
        assertEquals(status.getUser().getProfileBackgroundColor(), struct.get("User_ProfileBackgroundColor"), "ProfileBackgroundColor does not match.");
        assertEquals(status.getUser().getProfileTextColor(), struct.get("User_ProfileTextColor"), "ProfileTextColor does not match.");
        assertEquals(status.getUser().getProfileLinkColor(), struct.get("User_ProfileLinkColor"), "ProfileLinkColor does not match.");
        assertEquals(status.getUser().getProfileSidebarFillColor(), struct.get("User_ProfileSidebarFillColor"), "ProfileSidebarFillColor does not match.");
        assertEquals(status.getUser().getProfileSidebarBorderColor(), struct.get("User_ProfileSidebarBorderColor"), "ProfileSidebarBorderColor does not match.");
        assertEquals(status.getUser().isProfileUseBackgroundImage(), struct.get("User_ProfileUseBackgroundImage"), "ProfileUseBackgroundImage does not match.");
        assertEquals(status.getUser().isDefaultProfile(), struct.get("User_DefaultProfile"), "DefaultProfile does not match.");
        assertEquals(status.getUser().isShowAllInlineMedia(), struct.get("User_ShowAllInlineMedia"), "ShowAllInlineMedia does not match.");
        assertEquals(status.getUser().getFriendsCount(), struct.get("User_FriendsCount"), "FriendsCount does not match.");
        assertEquals(status.getUser().getCreatedAt(), struct.get("User_CreatedAt"), "CreatedAt does not match.");
        assertEquals(status.getUser().getFavouritesCount(), struct.get("User_FavouritesCount"), "FavouritesCount does not match.");
        assertEquals(status.getUser().getUtcOffset(), struct.get("User_UtcOffset"), "UtcOffset does not match.");
        assertEquals(status.getUser().getTimeZone(), struct.get("User_TimeZone"), "TimeZone does not match.");
        assertEquals(status.getUser().getProfileBackgroundImageURL(), struct.get("User_ProfileBackgroundImageURL"), "ProfileBackgroundImageURL does not match.");
        assertEquals(status.getUser().getProfileBackgroundImageUrlHttps(), struct.get("User_ProfileBackgroundImageUrlHttps"), "ProfileBackgroundImageUrlHttps does not match.");
        assertEquals(status.getUser().getProfileBannerURL(), struct.get("User_ProfileBannerURL"), "ProfileBannerURL does not match.");
        assertEquals(status.getUser().getProfileBannerRetinaURL(), struct.get("User_ProfileBannerRetinaURL"), "ProfileBannerRetinaURL does not match.");
        assertEquals(status.getUser().getProfileBannerIPadURL(), struct.get("User_ProfileBannerIPadURL"), "ProfileBannerIPadURL does not match.");
        assertEquals(status.getUser().getProfileBannerIPadRetinaURL(), struct.get("User_ProfileBannerIPadRetinaURL"), "ProfileBannerIPadRetinaURL does not match.");
        assertEquals(status.getUser().getProfileBannerMobileURL(), struct.get("User_ProfileBannerMobileURL"), "ProfileBannerMobileURL does not match.");
        assertEquals(status.getUser().getProfileBannerMobileRetinaURL(), struct.get("User_ProfileBannerMobileRetinaURL"), "ProfileBannerMobileRetinaURL does not match.");
        assertEquals(status.getUser().isProfileBackgroundTiled(), struct.get("User_ProfileBackgroundTiled"), "ProfileBackgroundTiled does not match.");
        assertEquals(status.getUser().getLang(), struct.get("User_Lang"), "Lang does not match.");
        assertEquals(status.getUser().getStatusesCount(), struct.get("User_StatusesCount"), "StatusesCount does not match.");
        assertEquals(status.getUser().isGeoEnabled(), struct.get("User_GeoEnabled"), "GeoEnabled does not match.");
        assertEquals(status.getUser().isVerified(), struct.get("User_Verified"), "Verified does not match.");
        assertEquals(status.getUser().isTranslator(), struct.get("User_Translator"), "Translator does not match.");
        assertEquals(status.getUser().getListedCount(), struct.get("User_ListedCount"), "ListedCount does not match.");
        assertEquals(status.getUser().isFollowRequestSent(), struct.get("User_FollowRequestSent"), "FollowRequestSent does not match.");

        assertEquals(status.getPlace().getName(), struct.get("Place_Name"), "Name does not match.");
        assertEquals(status.getPlace().getStreetAddress(), struct.get("Place_StreetAddress"), "StreetAddress does not match.");
        assertEquals(status.getPlace().getCountryCode(), struct.get("Place_CountryCode"), "CountryCode does not match.");
        assertEquals(status.getPlace().getId(), struct.get("Place_Id"), "Id does not match.");
        assertEquals(status.getPlace().getCountry(), struct.get("Place_Country"), "Country does not match.");
        assertEquals(status.getPlace().getPlaceType(), struct.get("Place_PlaceType"), "PlaceType does not match.");
        assertEquals(status.getPlace().getURL(), struct.get("Place_URL"), "URL does not match.");
        assertEquals(status.getPlace().getFullName(), struct.get("Place_FullName"), "FullName does not match.");

        assertEquals(status.getGeoLocation().getLatitude(), 1, struct.getFloat64("GeoLocation_Latitude"));
        assertEquals(status.getGeoLocation().getLongitude(), 1, struct.getFloat64("GeoLocation_Longitude"));

//        assertEquals(convert(status.getContributors()), struct.getArray("Contributors"), "Contributors does not match.");
//        assertEquals(convert(status.getWithheldInCountries()), struct.get("WithheldInCountries"), "WithheldInCountries does not match.");
    }

    void assertKey(Status status, Struct struct) {
        assertEquals(status.getId(), struct.get("Id"), "Id does not match.");
    }

    @Test
    public void convertStatus() {
        Status status = mockStatus();
        Struct struct = new Struct(StatusConverter.STATUS_SCHEMA);
        StatusConverter.convert(status, struct);
        assertStatus(status, struct);
    }

    @Test
    public void convertStatusKey() {
        Status status = mockStatus();
        Struct struct = new Struct(StatusConverter.STATUS_SCHEMA_KEY);
        StatusConverter.convertKey(status, struct);
        assertKey(status, struct);
    }

    void assertStatusDeletionNotice(StatusDeletionNotice statusDeletionNotice, Struct struct) {
        assertEquals(statusDeletionNotice.getStatusId(), struct.get("StatusId"), "StatusId does not match.");
        assertEquals(statusDeletionNotice.getUserId(), struct.get("UserId"), "UserId does not match.");
    }

    void assertStatusDeletionNoticeKey(StatusDeletionNotice statusDeletionNotice, Struct struct) {
        assertEquals(statusDeletionNotice.getStatusId(), struct.get("StatusId"), "StatusId does not match.");
    }

    @Test
    public void convertStatusDeletionNotice() {
        StatusDeletionNotice statusDeletionNotice = mockStatusDeletionNotice();
        Struct struct = new Struct(StatusConverter.SCHEMA_STATUS_DELETION_NOTICE);
        StatusConverter.convert(statusDeletionNotice, struct);
        assertStatusDeletionNotice(statusDeletionNotice, struct);
    }

    @Test
    public void convertKeyStatusDeletionNotice() {
        StatusDeletionNotice statusDeletionNotice = mockStatusDeletionNotice();
        Struct struct = new Struct(StatusConverter.SCHEMA_STATUS_DELETION_NOTICE_KEY);
        StatusConverter.convertKey(statusDeletionNotice, struct);
        assertStatusDeletionNoticeKey(statusDeletionNotice, struct);
    }
}
