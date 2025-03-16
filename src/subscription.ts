import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const ESPRESSO_TERMS = ['espresso', 'doppio', 'double shot']

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    // for (const post of ops.posts.creates) {
    //   if (post.record.tags && post.record.tags.length > 0) {
    //     console.log(post.record.tags)
    //   }
    // }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // Check for espresso-related text
        const containsEspressoText = ESPRESSO_TERMS.some((term) =>
          create.record.text.toLowerCase().includes(term.toLowerCase()),
        )

        // Check for espresso-related tags
        const containsEspressoTag =
          create.record.tags &&
          create.record.tags.some((tag) =>
            ESPRESSO_TERMS.some((term) =>
              tag.toLowerCase().includes(term.toLowerCase()),
            ),
          )

        // Check for espresso-related media description (if media exists and has a description)
        const containsEspressoInImageDesc =
          create.record.media &&
          // @ts-ignore
          create.record.media.description &&
          ESPRESSO_TERMS.some((term) =>
            // @ts-ignore
            create.record.media.description
              .toLowerCase()
              .includes(term.toLowerCase()),
          )

        // Logs for testing only
        // if ((create.record.tags || []).length > 0) {
        //   console.log('create.record.tags', create.record.tags)
        // }
        // if (Object.keys(create.record.media || {}).length > 0) {
        //   console.log('create.record.media', create.record.media)
        // }

        // Combine all checks: text, tags, and image media
        return (
          containsEspressoText ||
          containsEspressoTag ||
          containsEspressoInImageDesc
        )
      })
      .map((create) => {
        // map espresso-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
